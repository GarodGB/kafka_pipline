"""Stage 4: build the feature table (one row per account per month) from transactions_cleaned."""
import json
import os
import resource
import time

from pyspark.sql import SparkSession, Window, functions as F

T0 = time.time()
timings = {}

def timed(name, df):
    """Force Spark to run this step now, and record how long it took."""
    start = time.time()
    df = df.cache()
    n = df.count()
    timings[name] = round(time.time() - start, 2)
    print(f"[time] {name}: {timings[name]}s ({n} rows)")
    return df

# ---------- settings ----------
CONFIG_FILE = os.getenv("PIPELINE_CONFIG", "config/pipeline_config.json")
JDBC_URL = (f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:"
            f"{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'transactions')}")
JDBC_PROPS = {"user": os.getenv("PGUSER", "admin"), "password": os.environ["PGPASSWORD"],
              "driver": "org.postgresql.Driver"}

with open(CONFIG_FILE) as f:
    cfg = json.load(f)

spark = (SparkSession.builder
         .appName("build-features")
         .master("local[*]")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.sql.adaptive.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------- 1. read only the columns we need ----------
tx = (spark.read.jdbc(JDBC_URL, "transactions_cleaned", properties=JDBC_PROPS)
      .select("account_id", "txn_date_time", "billing_amount", "billing_currency",
              "merchant_name", "mcc_code"))

# ---------- 2. units: every money feature uses billing_amount, which is always USD ----------
currencies = [r.billing_currency for r in tx.select("billing_currency").distinct().collect()]
if currencies != ["USD"]:
    raise SystemExit(f"STOP: billing_currency is not only USD: {currencies}")

tx = (tx
      .withColumn("month", F.trunc("txn_date_time", "month"))     # 2022-07-19 14:02 -> 2022-07-01
      .withColumn("amount_usd", F.col("billing_amount"))          # one unit for all money: USD
      .drop("billing_amount", "billing_currency"))

# ---------- 3. spend groups: MCC code -> one of a few short groups (list lives in the config) ----------
fcfg = cfg["features"]
groups = spark.createDataFrame(
    [(code, group) for group, codes in fcfg["spend_groups"].items() for code in codes],
    ["mcc_code", "spend_group"])
tx = (tx.join(F.broadcast(groups), "mcc_code", "left")
        .withColumn("spend_group", F.coalesce("spend_group", F.lit(fcfg["default_group"]))))
tx = timed("1-3 read + units + groups", tx)                       # NEW (profiling)

# ---------- 4. monthly summary: one row per account per month (only months WITH transactions) ----------
amount = F.col("amount_usd")
group = F.col("spend_group")
spending = amount < 0                                    # money out; refunds (> 0) are money in

monthly = tx.groupBy("account_id", "month").agg(
    F.sum(F.when(amount > 0, amount).otherwise(0)).alias("credit"),     # money in  (refunds)
    F.sum(F.when(spending, -amount).otherwise(0)).alias("debit"),       # money out (spending)
    F.sum(amount).alias("net"),                                         # in - out
    F.count("*").alias("txn_count"),
    F.countDistinct("merchant_name").alias("merchant_count"),
    # spending per group (the groups come from the config)
    F.sum(F.when((group == "food") & spending, -amount).otherwise(0)).alias("spend_food"),
    F.sum(F.when((group == "travel") & spending, -amount).otherwise(0)).alias("spend_travel"),
    F.sum(F.when((group == "shopping") & spending, -amount).otherwise(0)).alias("spend_shopping"),
    F.sum(F.when((group == "bills") & spending, -amount).otherwise(0)).alias("spend_bills"),
    F.sum(F.when((group == "health_education") & spending, -amount).otherwise(0)).alias("spend_health_education"),
    F.sum(F.when((group == "cash_transfers") & spending, -amount).otherwise(0)).alias("spend_cash_transfers"),
    F.sum(F.when((group == "other") & spending, -amount).otherwise(0)).alias("spend_other"),
)
monthly = timed("4 monthly summary", monthly)                     # NEW (profiling)

# ---------- 5. fill the grid: EVERY account gets EVERY month (no transactions -> zeros) ----------
bounds = tx.agg(F.min("month").alias("first"), F.max("month").alias("last"))
all_months = bounds.select(F.explode(F.sequence("first", "last", F.expr("interval 1 month"))).alias("month"))
all_accounts = tx.select("account_id").distinct()
grid = all_accounts.crossJoin(all_months)                  # 300 accounts x 7 months = 2100 rows

monthly = (grid.join(monthly, ["account_id", "month"], "left")
               .fillna(0))                                  # a month with no transactions = 0 everywhere
monthly = timed("5 fill grid", monthly)                           # NEW (profiling)

# 6. balance = running total of net, per account, month by month
w_running = (Window.partitionBy("account_id")
             .orderBy("month")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
monthly = monthly.withColumn("balance", F.sum("net").over(w_running))

# 7a. balance from past months only
w = Window.partitionBy("account_id").orderBy("month")
w_prev3 = w.rowsBetween(-3, -1)          # the 3 months BEFORE this one

monthly = (monthly
    .withColumn("balance_lag1", F.lag("balance", 1).over(w))
    .withColumn("balance_lag2", F.lag("balance", 2).over(w))
    .withColumn("balance_lag3", F.lag("balance", 3).over(w))
    .withColumn("balance_mean_3m", F.avg("balance").over(w_prev3))
    .withColumn("balance_std_3m", F.stddev("balance").over(w_prev3))
    .withColumn("balance_change", F.col("balance_lag1") - F.col("balance_lag2")))

# 7b. activity from past months only
monthly = (monthly
    # money in / out / net: last month + average of last 3 months
    .withColumn("credit_lag1", F.lag("credit", 1).over(w))
    .withColumn("debit_lag1", F.lag("debit", 1).over(w))
    .withColumn("net_lag1", F.lag("net", 1).over(w))
    .withColumn("credit_avg_3m", F.avg("credit").over(w_prev3))
    .withColumn("debit_avg_3m", F.avg("debit").over(w_prev3))
    .withColumn("net_avg_3m", F.avg("net").over(w_prev3))
    # activity last month
    .withColumn("txn_count_lag1", F.lag("txn_count", 1).over(w))
    .withColumn("merchant_count_lag1", F.lag("merchant_count", 1).over(w))
    .withColumn("n_accounts", F.lit(1))
    # spending shape last month
    .withColumn("spend_food_lag1", F.lag("spend_food", 1).over(w))
    .withColumn("spend_travel_lag1", F.lag("spend_travel", 1).over(w))
    .withColumn("spend_shopping_lag1", F.lag("spend_shopping", 1).over(w))
    .withColumn("spend_bills_lag1", F.lag("spend_bills", 1).over(w))
    .withColumn("spend_health_education_lag1", F.lag("spend_health_education", 1).over(w))
    .withColumn("spend_cash_transfers_lag1", F.lag("spend_cash_transfers", 1).over(w))
    .withColumn("spend_other_lag1", F.lag("spend_other", 1).over(w)))

# 8. calendar (known in advance, so allowed)
monthly = (monthly
    .withColumn("month_of_year", F.month("month"))
    .withColumn("days_in_month", F.dayofmonth(F.last_day("month"))))

# 9. keep only keys + past features + calendar (point-in-time safe)
KEYS = ["account_id", "month"]
PAST = ["balance_lag1", "balance_lag2", "balance_lag3", "balance_mean_3m", "balance_std_3m",
        "balance_change",
        "credit_lag1", "debit_lag1", "net_lag1", "credit_avg_3m", "debit_avg_3m", "net_avg_3m",
        "txn_count_lag1", "merchant_count_lag1", "n_accounts",
        "spend_food_lag1", "spend_travel_lag1", "spend_shopping_lag1", "spend_bills_lag1",
        "spend_health_education_lag1", "spend_cash_transfers_lag1", "spend_other_lag1"]
CALENDAR = ["month_of_year", "days_in_month"]
features = monthly.select(KEYS + PAST + CALENDAR)
features = timed("6-9 balance + lags + calendar", features)        # NEW (profiling)
print("feature table:", len(features.columns), "columns")

# 10. save (overwrite = rerun-safe: the whole table is rebuilt every run)
start = time.time()                                               # NEW (profiling)
features = features.orderBy("account_id", "month")

(features.write.mode("overwrite").option("truncate", "true")
    .jdbc(JDBC_URL, "features_account_month", properties=JDBC_PROPS))

features.write.mode("overwrite").parquet("output/features_account_month.parquet")
print("saved: Postgres features_account_month + output/features_account_month.parquet")

# 10b. target for Stage 5: balance at the END of each month (kept in its own table, never a feature)
targets = monthly.select("account_id", "month", F.col("balance").alias("target_balance"))
(targets.write.mode("overwrite").option("truncate", "true")
    .jdbc(JDBC_URL, "targets_account_month", properties=JDBC_PROPS))
print("saved: Postgres targets_account_month")
timings["10 save"] = round(time.time() - start, 2)               # NEW (profiling)
print(f"[time] 10 save: {timings['10 save']}s")

# 11. profile report
jvm = spark.sparkContext._jvm
pools = jvm.java.lang.management.ManagementFactory.getMemoryPoolMXBeans()
spark_peak_mb = sum(p.getPeakUsage().getUsed() for p in pools
                    if str(p.getType()) == "Heap memory") / 1024**2
python_peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
slowest = max(timings, key=timings.get)

print("\n===== PROFILE =====")
print(f"total wall clock : {time.time() - T0:.1f}s")
print(f"slowest step     : {slowest} ({timings[slowest]}s)")
print(f"peak memory      : Spark {spark_peak_mb:.0f} MB | Python {python_peak_mb:.0f} MB")