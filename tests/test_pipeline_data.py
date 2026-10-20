"""Checks on the clean table AFTER the pipeline ran (load_raw -> spark_clean_job)."""
from decimal import Decimal



# ---------- counts ----------
def test_counts_add_up(q):
    read, cleaned, dropped = (q("SELECT rows_read FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1"),
                              q("SELECT rows_cleaned FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1"),
                              q("SELECT rows_dropped FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1"))
    assert read == cleaned + dropped


def test_every_raw_row_reaches_the_clean_table(q):
    assert q("SELECT COUNT(*) FROM transactions_cleaned") == q("SELECT COUNT(*) FROM raw_transactions")


def test_no_duplicate_txn_ids(q):
    assert q("SELECT COUNT(*) - COUNT(DISTINCT txn_id) FROM transactions_cleaned") == 0


def test_required_fields_never_empty(q):
    assert q("""SELECT COUNT(*) FROM transactions_cleaned
                WHERE txn_date_time IS NULL OR txn_amount IS NULL OR billing_amount IS NULL
                   OR fx_rate IS NULL OR txn_ccy IS NULL""") == 0


# ---------- trap rows: one real dirty row per rule ----------
def test_parentheses_amount_becomes_negative(q):        # raw "(808.41)"
    assert q("SELECT txn_amount FROM transactions_cleaned WHERE txn_id = '4704417'") == Decimal("-808.41")


def test_european_amount_format(q):                      # raw "176.711,00"
    assert q("SELECT txn_amount FROM transactions_cleaned WHERE txn_id = '4775863'") == 176711


def test_settle_before_txn_is_flagged(q):                # txn Aug 17, settle Aug 16
    assert q("SELECT flag_settle_before_txn FROM transactions_cleaned WHERE txn_id = '4706315'") is True


def test_fake_dates_become_missing(q):                   # 9 x 0000-00-00 + 2 x 1970 + 3 empty
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE flag_settle_missing") == 14
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE settle_date < '2000-01-01'") == 0


def test_fake_auth_codes_removed_and_flagged(q, cfg):
    fakes = tuple(cfg["auth"]["fake_codes"])
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE auth_code IN %s", (fakes,)) == 0
    assert q("SELECT COUNT(*) FROM transactions_cleaned WHERE flag_auth_suspicious") == 109


def test_payment_prefix_is_split(q):                     # raw "IZ *BP FUEL"
    assert q("SELECT merchant_name || '|' || payment_via FROM transactions_cleaned WHERE txn_id = '4700106'") == "BP FUEL|IZ"


# ---------- the FX formula: flag is true exactly when txn x fx is too far from billing ----------
def test_fx_formula_matches_the_flag(q, cfg):
    pct, absolute = cfg["amounts"]["fx_tolerance_pct"], cfg["amounts"]["fx_tolerance_abs"]
    wrong = q("""SELECT COUNT(*) FROM transactions_cleaned
                 WHERE flag_amount_mismatch <>
                       (ABS(txn_amount * fx_rate - billing_amount) > %s * ABS(billing_amount) + %s)""",
              (pct, absolute))
    assert wrong == 0


# ---------- rerun safety ----------
def test_writing_twice_does_not_duplicate(conn, q):
    """Write every clean row into transactions_cleaned AGAIN, two times, then undo the test."""
    before = q("SELECT COUNT(*) FROM transactions_cleaned")
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute("""INSERT INTO transactions_cleaned SELECT * FROM transactions_cleaned
                           ON CONFLICT (txn_id) DO UPDATE SET txn_amount = EXCLUDED.txn_amount""")
    assert q("SELECT COUNT(*) FROM transactions_cleaned") == before
    conn.rollback()                                       # leave the real table untouched


def test_every_successful_run_has_its_kafka_message(q):
    assert q("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'SUCCESS' AND payload IS NULL") == 0
