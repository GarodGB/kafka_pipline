"""Send the waiting messages (pipeline_runs with published_at = NULL) to Kafka."""
import json
import os
import time
from dotenv import load_dotenv
load_dotenv()

import psycopg2
from kafka import KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB = dict(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5433"),
          dbname=os.getenv("PGDATABASE", "transactions"),
          user=os.getenv("PGUSER", "admin"), password=os.environ["PGPASSWORD"])
MAX_TRIES = 3

producer = KafkaProducer(
    bootstrap_servers=KAFKA,
    acks="all",                                         # wait until Kafka has really stored it
    retries=5,                                          # Kafka library retries by itself too
    key_serializer=lambda k: k.encode("utf-8"),         # key: text -> bytes
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),   # value: dict -> JSON -> bytes
)

conn = psycopg2.connect(**DB)

# 1. find the runs whose message was never sent
with conn, conn.cursor() as cur:
    cur.execute("""SELECT run_id, topic, payload FROM pipeline_runs
                   WHERE status = 'SUCCESS' AND published_at IS NULL ORDER BY finished_at""")
    waiting = cur.fetchall()
print("letters waiting:", len(waiting))

# 2. send each one, wait for Kafka's receipt, then mark it as sent
for run_id, topic, payload in waiting:
    for attempt in range(1, MAX_TRIES + 1):
        try:
            receipt = producer.send(topic, key=run_id, value=payload).get(timeout=10)
            producer.flush()
            break
        except Exception as e:
            print(f"run {run_id}: try {attempt} failed: {e}")
            receipt = None
            time.sleep(2 * attempt)

    with conn, conn.cursor() as cur:
        if receipt is None:
            cur.execute("INSERT INTO audit_log (run_id, stage, event, details) VALUES (%s, 'emit', 'failed', %s)",
                        (run_id, json.dumps({"tries": MAX_TRIES})))
            print(f"run {run_id}: NOT sent, it stays waiting for next time")
            continue
        cur.execute("UPDATE pipeline_runs SET published_at = now() WHERE run_id = %s", (run_id,))
        cur.execute("INSERT INTO audit_log (run_id, stage, event, details) VALUES (%s, 'emit', 'published', %s)",
                    (run_id, json.dumps({"topic": receipt.topic, "partition": receipt.partition,
                                         "offset": receipt.offset})))
    print(f"run {run_id}: sent to {receipt.topic} | partition {receipt.partition} | offset {receipt.offset}")

producer.close()
conn.close()