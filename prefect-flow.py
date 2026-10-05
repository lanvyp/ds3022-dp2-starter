"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time
from datetime import datetime, timezone
 
import boto3
import duckdb
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
 
load_dotenv()
 
UVA_ID = os.environ["UVA_ID"]
SCATTER_BASE = "http://127.0.0.1:8000/api/scatter"
DB_PATH = os.getenv("DUCKDB_PATH", "dp2.duckdb")
EXPECTED = 21
TIMEOUT_S = int(os.getenv("DP2_TIMEOUT", "1200"))  # 900 s max delay + headroom
 
sqs = boto3.client("sqs")
 
 
@task(retries=0)  # never retry: every POST empties and refills the queue
def scatter(uva_id: str) -> str:
    logger = get_run_logger()
    resp = requests.post(f"{SCATTER_BASE}/{uva_id}", timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    logger.info(f"Scatter response: {payload}")
    return payload["sqs_url"]
 
 
@task
def init_db() -> None:
    """Create raw.fragments and clear rows from earlier runs (the POST resent everything)."""
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.fragments (
                order_no    VARCHAR,
                word        VARCHAR,
                received_at TIMESTAMP,
                message_id  VARCHAR
            )
        """)
        con.execute("DELETE FROM raw.fragments")
 
 
def queue_counts(queue_url: str) -> dict:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    counts = {
        "visible": int(attrs["ApproximateNumberOfMessages"]),
        "not_visible": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
        "delayed": int(attrs["ApproximateNumberOfMessagesDelayed"]),
    }
    counts["total"] = sum(counts.values())
    return counts
 
 
@task
def collect(queue_url: str) -> int:
    """Long-poll until all fragments are stored and the queue is empty, or time out."""
    logger = get_run_logger()
    seen: set[str] = set()
    start = time.monotonic()
 
    while True:
        elapsed = time.monotonic() - start
        counts = queue_counts(queue_url)
        logger.info(f"[{elapsed:5.0f}s] stored={len(seen)}/{EXPECTED} queue={counts}")
 
        # Done: every fragment stored AND nothing left on the queue (no dangling messages).
        if len(seen) >= EXPECTED and counts["total"] == 0:
            break
        if elapsed > TIMEOUT_S:
            raise TimeoutError(
                f"Gave up after {TIMEOUT_S}s with {len(seen)}/{EXPECTED} fragments; queue={counts}"
            )
 
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,          # long polling: returns early when a message appears
            VisibilityTimeout=60,
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])  # key is missing when nothing is visible
        if not messages:
            continue
 
        with duckdb.connect(DB_PATH) as con:
            for msg in messages:
                attrs = msg["MessageAttributes"]
                order_no = attrs["order_no"]["StringValue"]
                word = attrs["word"]["StringValue"]
 
                # 1) land first ...
                con.execute(
                    "INSERT INTO raw.fragments VALUES (?, ?, ?, ?)",
                    [order_no, word, datetime.now(timezone.utc), msg["MessageId"]],
                )
                # 2) ... then delete, so a crash can never lose a fragment
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                seen.add(order_no)
                logger.info(f"Stored + deleted fragment {order_no}: {word!r}")
 
    logger.info(f"Collected all {len(seen)} fragments in {time.monotonic() - start:.0f}s")
    return len(seen)
 
 
@flow(name="dp2-sqs-puzzle", log_prints=True)
def dp2_pipeline(uva_id: str = UVA_ID):
    queue_url = scatter(uva_id)
    init_db()
    collect(queue_url)
    # Stage 2 goes here: run_dbt() -> submit()
 
    with duckdb.connect(DB_PATH) as con:
        rows = con.execute(
            "SELECT order_no, word FROM raw.fragments ORDER BY CAST(order_no AS INTEGER)"
        ).fetchall()
    print(" ".join(w for _, w in rows))  # sanity check only; dbt does the real assembly
 
 
if __name__ == "__main__":
    dp2_pipeline()
