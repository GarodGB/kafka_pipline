import json
import os

from pyspark.sql import SparkSession, functions as F

# ---------- settings (from environment, with defaults for inside Docker) ----------
CONFIG_FILE = os.getenv("PIPELINE_CONFIG", "config/pipeline_config.json")
JDBC_URL = (f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:"
            f"{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'transactions')}")
JDBC_PROPS = {
    "user": os.getenv("PGUSER", "admin"),
    "password": os.environ["PGPASSWORD"],
    "driver": "org.postgresql.Driver",
}

# ---------- 1. open the rules book ----------
with open(CONFIG_FILE) as f:
    cfg = json.load(f)

# ---------- 2. start Spark (+ the Postgres driver) ----------
spark = (SparkSession.builder
         .appName("clean-transactions")
         .master("local[*]")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")   # NEW
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------- 3. read the whole raw table with JDBC ----------
raw = spark.read.jdbc(JDBC_URL, "raw_transactions", properties=JDBC_PROPS)
rows_read = raw.count()

print("config version:", cfg["config_version"])
print("rows read:", rows_read)







# ---------- 4. clean the dates ----------
def parse_date(col_name):
    """Try every format from the config. Return a timestamp, or null if nothing works."""
    text = F.trim(F.col(col_name))
    by_format = F.coalesce(*[F.to_timestamp(text, fmt) for fmt in cfg["dates"]["formats"]])
    by_epoch = F.when(text.rlike("^[0-9]{13}$"), F.timestamp_millis(text.cast("long")))
    ts = F.coalesce(by_epoch, by_format)
    return F.when(F.year(ts) >= cfg["dates"]["min_year"], ts)

df = (raw
      .withColumn("txn_date_clean", parse_date("txn_date_time"))
      .withColumn("settle_date_clean", parse_date("settle_date")))

# count: had a value, but we could not read it
def unparseable(raw_col, clean_col):
    return df.filter(F.col(raw_col).isNotNull() & F.col(clean_col).isNull()).count()

print("txn_date unparseable:", unparseable("txn_date_time", "txn_date_clean"))
print("settle_date unparseable:", unparseable("settle_date", "settle_date_clean"))
print("settle_date empty:", df.filter(F.col("settle_date").isNull()).count())

df.select("txn_id", "txn_date_time", "txn_date_clean",
          "settle_date", "settle_date_clean").show(8, truncate=False)






# ---------- 5. clean the amounts ----------
def parse_amount(col_name):
    """Turn a dirty money text into a decimal number, or null if it can't be read."""
    text = F.trim(F.col(col_name))
    negative = text.startswith("(") & text.endswith(")")
    digits = F.regexp_replace(text, r"[^0-9.,-]", "")
    european = digits.rlike(",[0-9]*$")
    fixed = (F.when(european, F.regexp_replace(F.regexp_replace(digits, r"\.", ""), ",", "."))
              .otherwise(F.regexp_replace(digits, ",", "")))
    number = fixed.cast("decimal(18,4)")
    return F.when(negative, -F.abs(number)).otherwise(number)

df = (df
      .withColumn("txn_amount_clean", parse_amount("txn_amount"))
      .withColumn("billing_amount_clean", parse_amount("billing_amount"))
      .withColumn("fx_rate_clean", F.trim(F.col("fx_rate")).cast("double").cast("decimal(24,12)")))

print("txn_amount unparseable:", unparseable("txn_amount", "txn_amount_clean"))
print("billing_amount unparseable:", unparseable("billing_amount", "billing_amount_clean"))
print("fx_rate unparseable:", unparseable("fx_rate", "fx_rate_clean"))

df.filter(~F.col("txn_amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$")) \
  .select("txn_id", "txn_amount", "txn_amount_clean").show(truncate=False)
  
  
  
  
  
# ---------- 6. clean the merchant name ----------
m = cfg["merchant"]
name = F.regexp_replace(F.upper(F.trim(F.col("merchant_name"))), r"\s+", " ")
before_star = F.trim(F.substring_index(name, "*", 1))
after_star = F.trim(F.regexp_extract(name, r"^[^*]*\*(.*)$", 1))
has_star = name.contains("*")
is_prefix = has_star & before_star.isin(m["known_prefixes"])

rename = F.create_map(*[F.lit(x) for kv in m["prefix_rename"].items() for x in kv])
df = df.withColumn("payment_via",
        F.when(is_prefix, F.coalesce(rename[before_star], before_star)).otherwise(F.lit("DIRECT")))

merchant = (F.when(is_prefix, after_star)
             .when(has_star, before_star)
             .otherwise(name))

JUNK = [r"\s*\*\w+$", r"\s+\d{4,}$", r"\s*#\d+$", r"\s+BR(\s*\d+)?$", r"\s*/\w+$"]
for _ in range(2):
    for pattern in JUNK:
        merchant = F.regexp_replace(merchant, pattern, "")
df = df.withColumn("merchant_short", F.trim(merchant))

# the map: 370 rules from the json file -> a small Spark table -> broadcast join
with open(m["map_file"]) as f:
    merchant_map = spark.createDataFrame(list(json.load(f).items()), ["map_from", "map_to"])

df = (df.join(F.broadcast(merchant_map), df["merchant_short"] == merchant_map["map_from"], "left")
        .withColumn("merchant_clean", F.coalesce("map_to", "merchant_short"))
        .drop("map_from", "map_to", "merchant_short"))

print("distinct merchants: raw", df.select("merchant_name").distinct().count(),
      "-> clean", df.select("merchant_clean").distinct().count())
df.groupBy("payment_via").count().orderBy(F.desc("count")).show()




# ---------- 7. clean the city and the country ----------
c = cfg["city"]
city = F.regexp_replace(F.upper(F.trim(F.col("merchant_city"))), r"\s+", " ")
city_map = F.create_map(*[F.lit(x) for kv in c["map"].items() for x in kv])

df = (df
      .withColumn("is_online", F.coalesce(city.isin(c["online_words"]), F.lit(False)))
      .withColumn("merchant_city_clean",
                  F.when(city.isNull() | (city == ""), F.lit(c["unknown_value"]))
                   .when(F.col("is_online"), F.lit(c["online_value"]))
                   .otherwise(F.coalesce(city_map[city], city))))

country = F.upper(F.trim(F.col("merchant_country")))
df = df.withColumn("merchant_country_clean",
                   F.when(country.isNull() | (country == ""), F.lit(c["unknown_value"])).otherwise(country))

# which country each city belongs to (105 cities) -> small table -> broadcast join
with open(c["country_file"]) as f:
    city_country = spark.createDataFrame(list(json.load(f).items()), ["cc_city", "expected_country"])

df = (df.join(F.broadcast(city_country), df["merchant_city_clean"] == city_country["cc_city"], "left")
        .withColumn("flag_country_mismatch",
                    F.coalesce(F.col("merchant_country_clean") != F.col("expected_country"), F.lit(False)))
        .drop("cc_city", "expected_country"))

df.groupBy("merchant_city_clean").count().orderBy(F.desc("count")).show(8)
print("online:", df.filter("is_online").count(),
      "| unknown city:", df.filter(F.col("merchant_city_clean") == c["unknown_value"]).count(),
      "| country mismatch:", df.filter("flag_country_mismatch").count())




# ---------- 8. terminal, auth code, MCC category ----------
terminal = F.upper(F.trim(F.col("terminal_id")))
is_missing_terminal = terminal.isNull() | (terminal == "") | terminal.isin(cfg["terminal"]["missing_values"])
df = (df
      .withColumn("has_terminal", ~is_missing_terminal)
      .withColumn("terminal_id_clean", F.when(is_missing_terminal, F.lit(None)).otherwise(terminal)))

auth = F.upper(F.trim(F.col("auth_code")))
is_fake_auth = auth.isNull() | (auth == "") | auth.isin(cfg["auth"]["fake_codes"])
df = (df
      .withColumn("flag_auth_suspicious", is_fake_auth)
      .withColumn("auth_code_clean", F.when(is_fake_auth, F.lit(None)).otherwise(auth)))

# MCC code -> category, from the mcc_reference table in Postgres (41 rows)
mcc = (spark.read.jdbc(JDBC_URL, "mcc_reference", properties=JDBC_PROPS)
            .withColumnRenamed("mcc_code", "ref_mcc")
            .withColumnRenamed("category", "mcc_category"))
df = (df.join(F.broadcast(mcc), F.trim(df["mcc_code"]) == mcc["ref_mcc"], "left")
        .drop("ref_mcc"))

print("no terminal:", df.filter(~F.col("has_terminal")).count(),
      "| fake auth:", df.filter("flag_auth_suspicious").count(),
      "| mcc without category:", df.filter(F.col("mcc_category").isNull()).count())
df.select("txn_id", "terminal_id", "terminal_id_clean", "has_terminal",
          "auth_code", "auth_code_clean", "mcc_code", "mcc_category").show(6, truncate=False)




# ---------- 9. the 13 flags ----------
from pyspark.sql import Window

r = cfg["rules"]
a = cfg["amounts"]
txn_day = F.to_date("txn_date_clean")
settle_day = F.to_date("settle_date_clean")

# expected billing = txn_amount x fx_rate ; allowed gap = 1% of billing + 0.01
expected_billing = F.col("txn_amount_clean") * F.col("fx_rate_clean")
gap = F.abs(expected_billing - F.col("billing_amount_clean"))
allowed = a["fx_tolerance_pct"] * F.abs(F.col("billing_amount_clean")) + a["fx_tolerance_abs"]

# the "usual" MCC of each merchant = the MCC it uses most often (looks at ALL rows of that merchant)
mcc_counts = df.groupBy("merchant_clean", "mcc_code").count()
rank = Window.partitionBy("merchant_clean").orderBy(F.desc("count"), "mcc_code")
usual_mcc = (mcc_counts.withColumn("rk", F.row_number().over(rank))
                       .filter("rk = 1")
                       .select("merchant_clean", F.col("mcc_code").alias("usual_mcc")))
df = df.join(usual_mcc, "merchant_clean", "left")

df = (df
  .withColumn("flag_settle_missing",     F.col("settle_date_clean").isNull())
  .withColumn("flag_settle_before_txn",  F.coalesce(settle_day < txn_day, F.lit(False)))
  .withColumn("flag_settle_lag",         F.coalesce(F.datediff(settle_day, txn_day) > cfg["dates"]["max_settle_lag_days"], F.lit(False)))
  .withColumn("flag_sign_mismatch",      F.signum("txn_amount_clean") != F.signum("billing_amount_clean"))
  .withColumn("flag_amount_mismatch",    F.coalesce(gap > allowed, F.lit(False)))
  .withColumn("flag_same_ccy_diff",      (F.col("txn_ccy") == F.col("billing_currency")) &
                                         (F.col("txn_amount_clean") != F.col("billing_amount_clean")))
  .withColumn("flag_status_not_matched", F.col("matches_status") != r["matched_status"])
  .withColumn("flag_atm_mcc_mismatch",   (F.col("processing_type") == r["atm_processing_type"]) & (F.col("mcc_code") != r["atm_mcc"]))
  .withColumn("flag_mcc_inconsistent",   F.col("mcc_code") != F.col("usual_mcc"))
  .withColumn("flag_city_unknown",       F.col("merchant_city_clean") == cfg["city"]["unknown_value"])
  .withColumn("flag_amount_outlier",     F.abs("billing_amount_clean") > a["outlier_usd"])
  .drop("usual_mcc"))
# flag_auth_suspicious (part 8) and flag_country_mismatch (part 7) already exist

FLAGS = [c for c in df.columns if c.startswith("flag_")]
df = df.withColumn("flags_count", sum(F.col(c).cast("int") for c in FLAGS))

flag_counts = df.select([F.sum(F.col(c).cast("int")).alias(c) for c in FLAGS]).first().asDict()
for name, n in flag_counts.items():
    print(f"{name:26} {n}")
print("rows with at least 1 flag:", df.filter("flags_count > 0").count(), "of", df.count())




# ---------- 10. input checks + quality gate ----------
import sys

q = cfg["quality_gate"]
d = cfg["dates"]
df = df.cache()   # keep the result in memory: the checks below reuse it many times

# remove repeats of the same txn_id (none today, but the rule must exist)
before_dedupe = df.count()
df = df.dropDuplicates(["txn_id"])
rows_cleaned = df.count()
rows_dropped = before_dedupe - rows_cleaned

def bad(condition):
    """count rows where the condition is true"""
    return F.sum(F.coalesce(condition, F.lit(True)).cast("int"))

txn_day = F.to_date("txn_date_clean")
checks = df.select(
    bad(F.col("txn_id").isNull()).alias("null_txn_id"),
    bad(~F.col("account_id").rlike("^[0-9]{9}$")).alias("bad_account_id"),
    bad((txn_day < F.lit(d["window_start"])) | (txn_day > F.lit(d["window_end"]))).alias("date_outside_window"),
    bad(F.col("txn_date_clean").isNull()).alias("missing_txn_date"),
    bad(F.col("txn_amount_clean").isNull()).alias("missing_txn_amount"),
    bad(F.col("billing_amount_clean").isNull()).alias("missing_billing_amount"),
    bad(F.col("fx_rate_clean").isNull()).alias("missing_fx_rate"),
    bad(F.col("txn_ccy").isNull()).alias("missing_currency"),
).first().asDict()

rows_flagged = df.filter("flags_count > 0").count()

# ---- the gate: collect every problem, then decide ----
problems = []
if rows_read != rows_cleaned + rows_dropped:
    problems.append("counts do not add up")
for name, n in checks.items():
    if n > q["max_unparseable_required"]:
        problems.append(f"{name}: {n} rows")
for name, n in flag_counts.items():
    rate = n / rows_cleaned
    if rate > q["max_flag_rate"]:
        problems.append(f"{name}: {rate:.1%} of rows (limit {q['max_flag_rate']:.0%})")

report = {
    "rows_read": rows_read, "rows_cleaned": rows_cleaned,
    "rows_dropped": rows_dropped, "rows_flagged": rows_flagged,
    "checks": checks, "flag_counts": flag_counts, "problems": problems,
}
print(json.dumps(report, indent=2))

if problems:
    print("QUALITY GATE FAILED -> nothing is written")
    sys.exit(1)
print("QUALITY GATE PASSED")



# ---------- 11. save: write straight into transactions_cleaned, then the bookkeeping ----------
import hashlib
from datetime import datetime, timezone
import psycopg2

# 11a. the run id: same file + same config  ->  same run id
files = raw.select("source_file", "file_hash").distinct().orderBy("file_hash").collect()
source_file = ",".join(r.source_file for r in files)
file_hash = ",".join(r.file_hash for r in files)
run_id = hashlib.sha256(f"{file_hash}|{cfg['config_version']}".encode()).hexdigest()[:16]
print("run_id:", run_id)

# 11b. the final table: keep only clean values, with the final column names
final = df.select(
    "txn_id", "account_id",
    F.col("txn_date_clean").alias("txn_date_time"),
    F.col("settle_date_clean").alias("settle_date"),
    "txn_type",
    F.col("txn_amount_clean").alias("txn_amount"),
    "txn_ccy",
    F.col("billing_amount_clean").alias("billing_amount"),
    "billing_currency",
    F.col("fx_rate_clean").alias("fx_rate"),
    F.col("merchant_clean").alias("merchant_name"),
    "payment_via", "mcc_code", "mcc_category",
    F.col("merchant_country_clean").alias("merchant_country"),
    F.col("merchant_city_clean").alias("merchant_city"),
    "processing_code", "processing_type",
    F.col("terminal_id_clean").alias("terminal_id"),
    F.col("auth_code_clean").alias("auth_code"),
    "matches_status", "is_online", "has_terminal",
    *FLAGS, "flags_count",
    F.lit(run_id).alias("run_id"),
    F.current_timestamp().alias("cleaned_at"),
    F.lit(cfg["config_version"]).alias("config_version"),
)

# 11c. write STRAIGHT into transactions_cleaned (txn_id is unique: new -> insert, existing -> update)
for c in ["txn_date_time", "settle_date", "cleaned_at"]:            # timestamps -> plain text, Postgres reads them back
    final = final.withColumn(c, F.date_format(c, "yyyy-MM-dd HH:mm:ss"))

cols = final.columns
upsert_sql = (f"INSERT INTO transactions_cleaned ({', '.join(cols)}) VALUES %s "
              f"ON CONFLICT (txn_id) DO UPDATE SET "
              + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "txn_id"))
PG = dict(host=os.getenv("PGHOST", "postgres"), port=os.getenv("PGPORT", "5432"),
          dbname=os.getenv("PGDATABASE", "transactions"),
          user=JDBC_PROPS["user"], password=JDBC_PROPS["password"])

def upsert_partition(rows):
    """Runs on each Spark worker: sends its pile of rows to Postgres in one go."""
    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect(**PG)
    with conn, conn.cursor() as cur:
        execute_values(cur, upsert_sql, [tuple(r) for r in rows], page_size=500)
    conn.close()

final.foreachPartition(upsert_partition)
print("written to transactions_cleaned:", rows_cleaned, "rows")

# 11d. one SQL transaction: check + bookkeeping. All succeed, or nothing changes.
expected_sum = final.agg(F.sum("txn_amount")).first()[0]

payload = {
    "run_id": run_id, "schema_version": "1.0",
    "source_file": source_file, "target_table": "transactions_cleaned",
    "rows_read": rows_read, "rows_cleaned": rows_cleaned,
    "rows_dropped": rows_dropped, "rows_flagged": rows_flagged,
    "flag_counts": flag_counts,
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

conn = psycopg2.connect(**PG)
with conn:                       # commit at the end, or rollback if anything fails
    with conn.cursor() as cur:
        # reconcile: does the table now hold exactly what Spark produced?
        cur.execute("SELECT COUNT(*), SUM(txn_amount) FROM transactions_cleaned WHERE run_id = %s", (run_id,))
        db_count, db_sum = cur.fetchone()
        if db_count != rows_cleaned or db_sum != expected_sum:
            raise RuntimeError(f"reconcile failed: table {db_count}/{db_sum} vs spark {rows_cleaned}/{expected_sum}")

        # the run report + the Kafka message, in ONE row of pipeline_runs (published_at stays NULL = not sent yet)
        cur.execute("""
            INSERT INTO pipeline_runs (run_id, status, source_file, file_hash, config_version,
                rows_read, rows_cleaned, rows_dropped, rows_flagged, flag_counts, report, finished_at,
                topic, payload)
            VALUES (%s, 'SUCCESS', %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET status = 'SUCCESS', report = EXCLUDED.report,
                flag_counts = EXCLUDED.flag_counts, finished_at = now()
        """, (run_id, source_file, file_hash, cfg["config_version"], rows_read, rows_cleaned,
              rows_dropped, rows_flagged, json.dumps(flag_counts), json.dumps(report),
              cfg["kafka"]["topic"], json.dumps(payload)))

        cur.execute("INSERT INTO audit_log (run_id, stage, event, details) VALUES (%s, 'write', 'upserted', %s)",
                    (run_id, json.dumps({"rows_written": rows_cleaned, "db_count": db_count})))
conn.close()

print(f"table has {db_count} rows for this run | sum {db_sum}")
print("SAVED OK")
spark.stop()
