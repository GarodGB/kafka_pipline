"""Consumer tests: no real Kafka needed, we hand-make the messages."""
import collections
import json

import event_consumer as ec

Msg = collections.namedtuple("Msg", "key value offset")


class FakeDLQ:
    """Pretends to be Kafka's dead letter topic, remembers what it got."""
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None, headers=None):
        self.sent.append(dict(headers)["reason"].decode())
        return self

    def get(self, timeout=None):
        return None


def good_event(q):
    return q("SELECT payload FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1")


# ---------- validate(): pure checks ----------
def test_validate_accepts_the_real_event(q):
    event, reason = ec.validate(json.dumps(good_event(q)).encode())
    assert reason is None


def test_validate_rejects_broken_json():
    assert ec.validate(b"{oops")[1] == "not valid JSON"


def test_validate_rejects_missing_field(q):
    event = good_event(q)
    del event["rows_read"]
    assert ec.validate(json.dumps(event).encode())[1] == "missing field: rows_read"


def test_validate_rejects_bad_math(q):
    event = dict(good_event(q), rows_dropped=5)
    assert ec.validate(json.dumps(event).encode())[1] == "rows_read != rows_cleaned + rows_dropped"


# ---------- handle(): with the real database ----------
def test_same_event_twice_is_processed_once(conn, q):
    event = good_event(q)
    with conn, conn.cursor() as cur:                      # start clean for this run
        cur.execute("DELETE FROM processed_events WHERE run_id = %s", (event["run_id"],))
    msg = Msg(b"k", json.dumps(event).encode(), 0)
    dlq = FakeDLQ()
    ec.handle(msg, conn, dlq)                             # 1st delivery -> processed
    ec.handle(msg, conn, dlq)                             # 2nd delivery -> skipped
    assert q("SELECT COUNT(*) FROM processed_events WHERE run_id = %s", (event["run_id"],)) == 1
    assert dlq.sent == []


def test_malformed_event_goes_to_dead_letter(conn, q):
    before = q("SELECT COUNT(*) FROM rejected_events")
    dlq = FakeDLQ()
    ec.handle(Msg(b"k", b"hello", 99), conn, dlq)
    assert dlq.sent == ["not valid JSON"]
    assert q("SELECT COUNT(*) FROM rejected_events") == before + 1


def test_lying_event_is_rejected(conn, q):
    event = dict(good_event(q), run_id="no-such-run")
    dlq = FakeDLQ()
    ec.handle(Msg(b"k", json.dumps(event).encode(), 100), conn, dlq)
    assert dlq.sent == ["letter says 2296 rows, table has 0"]
    assert q("SELECT COUNT(*) FROM processed_events WHERE run_id = 'no-such-run'") == 0
