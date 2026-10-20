"""Stage 1: load the Excel file into raw_transactions, untouched."""
import hashlib
import json
import os
import sys
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

EXCEL_FILE = "data/synthetic_dirty_transactions_v4.xlsx"
DB = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5433")),
    user=os.getenv("PGUSER", "admin"),
    password=os.environ["PGPASSWORD"],
    dbname=os.getenv("PGDATABASE", "transactions"),
)
EXPECTED_COLUMNS = [
    "ACCOUNT_ID", "TXN_ID", "TXN_DATE_TIME", "SETTLE_DATE", "TXN_TYPE", "TXN_AMOUNT",
    "TXN_CCY", "BILLING_AMOUNT", "BILLING_CURRENCY", "FX_RATE", "MERCHANT_NAME",
    "MCC_CODE", "MERCHANT_COUNTRY", "MERCHANT_CITY", "PROCESSING_CODE",
    "PROCESSING_TYPE", "TERMINAL_ID", "AUTH_CODE", "MATCHES_STATUS",
]


def file_hash(path):
    """SHA-256 fingerprint of the file: same file = same hash."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def main():
    # 1. read both sheets as TEXT (untouched)
    txns = pd.read_excel(EXCEL_FILE, sheet_name="Transactions", dtype=str)
    mcc = pd.read_excel(EXCEL_FILE, sheet_name="MCC_Reference", dtype=str)
    txns = txns.astype(object).where(txns.notna(), None)      # NaN -> NULL

    # 2. schema check: all 19 columns must be there, or stop
    missing = [c for c in EXPECTED_COLUMNS if c not in txns.columns]
    if missing:
        sys.exit(f"STOP: missing columns {missing}")

    # 3. fingerprint of the input
    fhash = file_hash(EXCEL_FILE)
    source = os.path.basename(EXCEL_FILE)

    # 4. insert (a rerun adds nothing, thanks to ON CONFLICT on txn_id)
    cols = [c.lower() for c in EXPECTED_COLUMNS] + ["source_file", "file_hash"]
    rows = [tuple(r[c] for c in EXPECTED_COLUMNS) + (source, fhash)
            for r in txns.to_dict("records")]

    conn = psycopg2.connect(**DB)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw_transactions")
        before = cur.fetchone()[0]
        execute_values(
            cur,
            f"INSERT INTO raw_transactions ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (txn_id) DO NOTHING",
            rows,
        )
        execute_values(
            cur,
            "INSERT INTO mcc_reference (mcc_code, category) VALUES %s "
            "ON CONFLICT (mcc_code) DO UPDATE SET category = EXCLUDED.category",
            list(mcc[["MCC_CODE", "CATEGORY"]].itertuples(index=False, name=None)),
        )

        # 5. count check: raw table must have every source row
        cur.execute("SELECT count(*) FROM raw_transactions")
        in_table = cur.fetchone()[0]
        inserted = in_table - before
        status = "OK" if in_table == len(txns) else "COUNT_MISMATCH"

        # 6. audit
        details = {"source_file": source, "file_hash": fhash, "source_rows": len(txns),
                   "inserted_now": inserted, "rows_in_table": in_table,
                   "mcc_codes": len(mcc), "status": status}
        cur.execute("INSERT INTO audit_log (stage, event, details) VALUES (%s, %s, %s)",
                    ("load", "raw_loaded", json.dumps(details)))
    conn.close()

    print(json.dumps(details, indent=2))
    if status != "OK":
        sys.exit("STOP: raw row count does not match the source file")


if __name__ == "__main__":
    main()