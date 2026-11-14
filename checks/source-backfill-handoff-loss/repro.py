# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["confluent-kafka==2.6.1", "psycopg[binary]==3.2.3"]
# ///
"""Reproduces rows lost at a source backfill hand-off (Kafka EOF progress while SourceCachingUp)."""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid

import psycopg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA = "localhost:19092"
RW = "host=localhost port=14566 user=root dbname=dev"
TOPIC = "events"
PER_TXN = 10


def sql(conn, statement):
    print(f"  sql> {statement}")
    with conn.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall() if cur.description else None


def connect():
    for _ in range(120):
        try:
            conn = psycopg.connect(RW, autocommit=True)
            sql(conn, "SELECT 1")
            return conn
        except psycopg.OperationalError:
            time.sleep(1)
    sys.exit("RisingWave did not start")


def create_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for _ in range(60):
        try:
            admin.list_topics(timeout=2)
            break
        except Exception:
            time.sleep(1)
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])[TOPIC].result()


def produce(ids):
    producer = Producer({"bootstrap.servers": KAFKA, "transactional.id": f"repro-{uuid.uuid4()}"})
    producer.init_transactions()
    for start in range(0, len(ids), PER_TXN):
        producer.begin_transaction()
        for i in ids[start : start + PER_TXN]:
            producer.produce(TOPIC, json.dumps({"id": i}).encode(), partition=0)
        producer.commit_transaction()
    print(f"  produced ids {ids[0]}..{ids[-1]} in {len(ids) // PER_TXN} transactions")


def wait_for(conn, query, expected, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if sql(conn, query)[0][0] == expected:
            return
        time.sleep(2)
    sys.exit(f"timed out waiting for {query} = {expected}")


def ranges(ids):
    out, start, prev = [], None, None
    for i in ids:
        if start is None:
            start = prev = i
        elif i == prev + 1:
            prev = i
        else:
            out.append((start, prev))
            start = prev = i
    if start is not None:
        out.append((start, prev))
    return ", ".join(f"{a}..{b}" if a != b else f"{a}" for a, b in out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stall-ms", type=int, default=10000, help="barrier interval while the new view hands off")
    args = parser.parse_args()

    conn = connect()
    print(sql(conn, "SELECT version()")[0][0])
    create_topic()

    print("1. Batch A, then a live view that is caught up and forwarding from the shared source")
    produce(list(range(0, 100)))
    sql(conn, f"CREATE SOURCE events (id INT) WITH (connector = 'kafka', topic = '{TOPIC}', "
        "properties.bootstrap.server = 'kafka:9092', scan.startup.mode = 'earliest') FORMAT PLAIN ENCODE JSON")
    sql(conn, "CREATE MATERIALIZED VIEW mv_live AS SELECT id FROM events")
    wait_for(conn, "SELECT count(*) FROM mv_live", 100)

    print("2. The shared source stops delivering (prod: the 9 s upstream stall)")
    sql(conn, "ALTER SOURCE events SET SOURCE_RATE_LIMIT = 0")

    print("3. Create the new view; its backfill reader records target_offset = end of batch A")
    sql(conn, "SET background_ddl = true")
    sql(conn, "SET backfill_rate_limit = 0")
    sql(conn, "CREATE MATERIALIZED VIEW mv_new AS SELECT id FROM events")
    wait_for(conn, "SELECT count(*) FROM rw_catalog.rw_ddl_progress", 1)
    time.sleep(3)

    print("4. Batch B arrives in Kafka; the shared source has not delivered it")
    produce(list(range(100, 200)))

    print(f"5. Barriers {args.stall_ms} ms apart; the backfill reader reads A and B and reaches partition EOF")
    sql(conn, f"ALTER SYSTEM SET barrier_interval_ms = {args.stall_ms}")
    time.sleep(2)
    sql(conn, "ALTER MATERIALIZED VIEW mv_new SET BACKFILL_RATE_LIMIT = DEFAULT")
    time.sleep(args.stall_ms / 1000 * 2 + 2)
    sql(conn, "ALTER SYSTEM SET barrier_interval_ms = 1000")

    print("6. The shared source resumes and delivers B, then batch C")
    sql(conn, "ALTER SOURCE events SET SOURCE_RATE_LIMIT = DEFAULT")
    produce(list(range(200, 300)))
    wait_for(conn, "SELECT count(*) FROM mv_live", 300)
    wait_for(conn, "SELECT count(*) FROM rw_catalog.rw_ddl_progress", 0)
    time.sleep(3)

    live = {r[0] for r in sql(conn, "SELECT id FROM mv_live")}
    new = {r[0] for r in sql(conn, "SELECT id FROM mv_new")}
    missing = sorted(live - new)
    extra = sorted(new - live)
    print()
    print(f"mv_live rows: {len(live)}   mv_new rows: {len(new)}")
    print(f"missing from mv_new: {len(missing)} [{ranges(missing)}]")
    print(f"extra in mv_new: {len(extra)} [{ranges(extra)}]")

    logs = subprocess.run(
        ["docker", "compose", "--profile", "local", "logs", "--no-color", os.environ.get("RW_SERVICE", "risingwave")],
        capture_output=True, text=True, cwd=sys.path[0],
    ).stdout
    print("\nhand-off log lines:")
    for line in logs.splitlines():
        if any(k in line for k in ("received kafka partition EOF", "applied split progress", "source backfill finished")):
            print("  " + line.split("risingwave_", 1)[-1][:400])

    if missing:
        print("\nREPRODUCED: the new view lost rows at its backfill hand-off")
        sys.exit(1)
    print("\nNOT REPRODUCED: no rows lost")


if __name__ == "__main__":
    main()
