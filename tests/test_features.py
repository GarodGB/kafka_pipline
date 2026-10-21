"""Stage 4 tests: checks on features_account_month AFTER build_features.py ran.

Idea: Spark built the features. Here we compute the same numbers AGAIN with plain SQL
straight from transactions_cleaned, and check that both agree.
"""

PAST_FEATURES = ["balance_lag1", "balance_lag2", "balance_lag3", "balance_mean_3m", "balance_std_3m",
                 "balance_change", "credit_lag1", "debit_lag1", "net_lag1", "credit_avg_3m",
                 "debit_avg_3m", "net_avg_3m", "txn_count_lag1", "merchant_count_lag1",
                 "spend_food_lag1", "spend_travel_lag1", "spend_shopping_lag1", "spend_bills_lag1",
                 "spend_health_education_lag1", "spend_cash_transfers_lag1", "spend_other_lag1"]
ALLOWED = ["account_id", "month", "n_accounts", "month_of_year", "days_in_month"] + PAST_FEATURES


# ---------- shape: one row per account per month ----------
def test_one_row_per_account_per_month(q):
    accounts = q("SELECT COUNT(DISTINCT account_id) FROM transactions_cleaned")
    months = q("SELECT COUNT(DISTINCT month) FROM features_account_month")
    assert q("SELECT COUNT(*) FROM features_account_month") == accounts * months      # 300 x 7
    assert q("""SELECT COUNT(*) - COUNT(DISTINCT (account_id, month))
                FROM features_account_month""") == 0                                  # no duplicates


# ---------- aggregation: last month's numbers = SQL recount from the transactions ----------
def test_last_month_count_and_debit_match_sql(q):
    wrong = q("""
        WITH m AS (
            SELECT account_id, date_trunc('month', txn_date_time)::date AS month,
                   COUNT(*) AS n,
                   SUM(CASE WHEN billing_amount < 0 THEN -billing_amount ELSE 0 END) AS debit
            FROM transactions_cleaned GROUP BY 1, 2)
        SELECT COUNT(*) FROM features_account_month f
        LEFT JOIN m ON m.account_id = f.account_id
                   AND m.month = (f.month - INTERVAL '1 month')::date
        WHERE f.txn_count_lag1 IS NOT NULL
          AND (f.txn_count_lag1 <> COALESCE(m.n, 0)
               OR ABS(f.debit_lag1 - COALESCE(m.debit, 0)) > 0.01)""")
    assert wrong == 0


def test_spend_groups_add_up_to_debit(q):
    """Every spent dollar lands in exactly one group: the 7 groups sum to debit."""
    wrong = q("""SELECT COUNT(*) FROM features_account_month
                 WHERE debit_lag1 IS NOT NULL
                   AND ABS(debit_lag1 - (spend_food_lag1 + spend_travel_lag1 + spend_shopping_lag1
                        + spend_bills_lag1 + spend_health_education_lag1
                        + spend_cash_transfers_lag1 + spend_other_lag1)) > 0.01""")
    assert wrong == 0


def test_known_account_september(q):
    """Hand-checked example: account 501893368."""
    row = "FROM features_account_month WHERE account_id = '501893368' AND month = '2022-09-01'"
    assert abs(float(q(f"SELECT balance_lag1 {row}")) - (-46.56)) < 0.01      # end of August
    assert abs(float(q(f"SELECT balance_lag3 {row}")) - (-42.55)) < 0.01      # end of June
    assert abs(float(q(f"SELECT debit_avg_3m {row}")) - 1.3367) < 0.01        # (0 + 4.01 + 0) / 3


# ---------- point-in-time: a row only uses data from BEFORE its own month ----------
def test_balance_uses_only_earlier_transactions(q):
    """balance_lag1 = sum of ALL transactions strictly before the row's month. Nothing from the month itself."""
    wrong = q("""SELECT COUNT(*) FROM features_account_month f
                 WHERE f.balance_lag1 IS NOT NULL
                   AND ABS(f.balance_lag1 - (SELECT COALESCE(SUM(t.billing_amount), 0)
                                             FROM transactions_cleaned t
                                             WHERE t.account_id = f.account_id
                                               AND t.txn_date_time < f.month)) > 0.01""")
    assert wrong == 0


def test_first_month_has_no_history(q):
    """The first month has nothing before it, so every past feature must be empty (NULL), not invented."""
    nulls = " AND ".join(f"{c} IS NULL" for c in PAST_FEATURES)
    first = "(SELECT MIN(month) FROM features_account_month)"
    assert q(f"SELECT COUNT(*) FROM features_account_month WHERE month = {first} AND NOT ({nulls})") == 0


def test_no_current_month_columns_in_table(q):
    """Only keys + past features + calendar. Columns like balance / debit / net (this month) must be gone."""
    cols = set(q("""SELECT array_agg(column_name::text) FROM information_schema.columns
                            WHERE table_name = 'features_account_month'"""))
    assert cols == set(ALLOWED)


# ---------- units: every money feature is USD ----------
def test_billing_currency_is_only_usd(q):
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE billing_currency <> 'USD'") == 0


def test_money_uses_billing_amount_not_txn_amount(q):
    """There ARE foreign-currency transactions (EUR, LBP...). If Spark had used txn_amount,
    the aggregation test above would fail on them. Here we just prove they exist, so that test means something."""
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE txn_ccy <> 'USD'") > 0


def test_money_in_and_out_are_never_negative(q):
    assert q("""SELECT COUNT(*) FROM features_account_month
                WHERE credit_lag1 < 0 OR debit_lag1 < 0 OR credit_avg_3m < 0 OR debit_avg_3m < 0""") == 0
