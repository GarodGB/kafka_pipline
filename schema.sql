-- =========================================================
-- 0. remove the old test tables (they used a SERIAL id)
-- =========================================================
DROP TABLE IF EXISTS transactions_cleaned;
DROP TABLE IF EXISTS raw_transactions;

-- =========================================================
-- 1. RAW: the Excel, untouched, all text
-- =========================================================
CREATE TABLE IF NOT EXISTS raw_transactions (
    txn_id            TEXT PRIMARY KEY,      -- the key: a rerun can't duplicate
    account_id        TEXT,
    txn_date_time     TEXT,
    settle_date       TEXT,
    txn_type          TEXT,
    txn_amount        TEXT,
    txn_ccy           TEXT,
    billing_amount    TEXT,
    billing_currency  TEXT,
    fx_rate           TEXT,
    merchant_name     TEXT,
    mcc_code          TEXT,
    merchant_country  TEXT,
    merchant_city     TEXT,
    processing_code   TEXT,
    processing_type   TEXT,
    terminal_id       TEXT,
    auth_code         TEXT,
    matches_status    TEXT,
    -- where the row came from
    source_file       TEXT,
    file_hash         TEXT,
    loaded_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mcc_reference (
    mcc_code  TEXT PRIMARY KEY,
    category  TEXT
);

-- =========================================================
-- 2. CLEAN: real types + flags + lineage
-- =========================================================
CREATE TABLE IF NOT EXISTS transactions_cleaned (
    txn_id            TEXT PRIMARY KEY,
    account_id        TEXT NOT NULL,
    txn_date_time     TIMESTAMP NOT NULL,
    settle_date       TIMESTAMP,
    txn_type          TEXT,
    txn_amount        NUMERIC(18,4) NOT NULL,
    txn_ccy           TEXT,
    billing_amount    NUMERIC(18,4),
    billing_currency  TEXT,
    fx_rate           NUMERIC(24,12),
    merchant_name     TEXT,
    payment_via       TEXT,
    mcc_code          TEXT,
    mcc_category      TEXT,
    merchant_country  TEXT,
    merchant_city     TEXT,
    processing_code   TEXT,
    processing_type   TEXT,
    terminal_id       TEXT,
    auth_code         TEXT,
    matches_status    TEXT,
    -- info (not problems)
    is_online         BOOLEAN,
    has_terminal      BOOLEAN,
    -- flags (problems found, row is kept)
    flag_settle_missing      BOOLEAN,
    flag_settle_before_txn   BOOLEAN,
    flag_settle_lag          BOOLEAN,
    flag_sign_mismatch       BOOLEAN,
    flag_amount_mismatch     BOOLEAN,
    flag_same_ccy_diff       BOOLEAN,
    flag_status_not_matched  BOOLEAN,
    flag_atm_mcc_mismatch    BOOLEAN,
    flag_mcc_inconsistent    BOOLEAN,
    flag_auth_suspicious     BOOLEAN,
    flag_city_unknown        BOOLEAN,
    flag_country_mismatch    BOOLEAN,
    flag_amount_outlier      BOOLEAN,
    flags_count              INT,
    -- lineage: which run wrote this row
    run_id            TEXT NOT NULL,
    cleaned_at        TIMESTAMP NOT NULL DEFAULT now(),
    config_version    TEXT NOT NULL
);

-- =========================================================
-- 3. PIPELINE BOOKKEEPING
-- =========================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    status          TEXT NOT NULL,           -- STARTED / SUCCESS / FAILED
    source_file     TEXT,
    file_hash       TEXT,
    config_version  TEXT,
    rows_read       INT,
    rows_cleaned    INT,
    rows_dropped    INT,
    rows_flagged    INT,
    flag_counts     JSONB,
    report          JSONB,
    started_at      TIMESTAMP DEFAULT now(),
    finished_at     TIMESTAMP,
    -- the Kafka message for this run (the "outbox")
    topic           TEXT,
    payload         JSONB,
    published_at    TIMESTAMP                -- NULL = not sent to Kafka yet
);

CREATE TABLE IF NOT EXISTS processed_events (
    run_id        TEXT PRIMARY KEY,          -- a run is handled only once
    rows_cleaned  INT,
    processed_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_events (
    id           BIGSERIAL PRIMARY KEY,
    reason       TEXT NOT NULL,
    payload      TEXT,
    received_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGSERIAL PRIMARY KEY,
    run_id     TEXT,
    stage      TEXT NOT NULL,                -- load / clean / write / emit / consume
    event      TEXT NOT NULL,
    details    JSONB,
    logged_at  TIMESTAMP DEFAULT now()
);