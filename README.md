# Kafka Transaction Pipeline

## Requirements

- Docker Desktop
- Python 3.11+
- DBeaver (or any Postgres client)

## Setup (once)

1. Install the Python libraries:
   ```
   pip install -r requirements.txt
   ```
2. Create your settings file:
   ```
   copy .env.example .env
   ```
   Then open `.env` and set your own `PGPASSWORD`.
3. Put the Excel file here (it is not in the repo):
   ```
   data/synthetic_dirty_transactions_v4.xlsx
   ```
4. Start Kafka, Postgres and Spark:
   ```
   docker compose up -d
   ```
5. Create the tables: open `schema.sql` in DBeaver, connect to database `transactions`
   (localhost:5433), and run it.
6. Create the Kafka topics:
   ```
   python create_topics.py
   ```

## Run (in this order)

| Step | Command | What it does |
|---|---|---|
| 1 | `python load_raw.py` | Loads the Excel into `raw_transactions` |
| 2 | `docker exec -it spark python spark_clean_job.py` | Cleans the data into `transactions_cleaned` |
| 3 | `python publish_outbox.py` | Sends the run message to Kafka |
| 4 | `python event_consumer.py` | Receives and checks the message (Ctrl + C to stop) |

All steps are safe to run again.

## Tests

```
python -m pytest
```

Run the pipeline first (steps 1 to 3). The tests read the results from Postgres.
