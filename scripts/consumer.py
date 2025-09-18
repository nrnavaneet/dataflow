# -*- coding: utf-8 -*-
#
# File: consumer.py
# Description: Kafka consumer for ingesting stock trading events and
# flushing to S3 as Parquet files.
# This script is part of an open-source data pipeline for benchmarking and analytics.
#
# Usage: Run as a standalone script to start multiple consumer processes.
# License: MIT

from confluent_kafka import Consumer
from dotenv import load_dotenv
from datetime import datetime
import orjson
import time
import os
import multiprocessing as mp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import random

load_dotenv()

BROKERS = "localhost:8097,localhost:8098,localhost:8099"
TOPIC = "stock-prices"
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")

# batching thresholds
BATCH_SIZE = 50000
FLUSH_INTERVAL = 5  # seconds

# s3 client
fs = s3fs.S3FileSystem(
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    client_kwargs={"region_name": REGION},
)


def flush_to_s3(records, worker_id, attempt=1, max_attempts=3):
    """
    Flush a batch of records to S3 as a Parquet file, with retry logic.

    Args:
        records (list): List of stock event dicts to write.
        worker_id (int): ID of the consumer worker process.
        attempt (int): Current retry attempt (default 1).
        max_attempts (int): Maximum retry attempts (default 3).

    Returns:
        bool: True if flush succeeded, False otherwise.

    On failure, retries up to max_attempts before giving up.
    """
    """Flush batch to S3 with retries"""
    if not records:
        return False

    try:
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)

        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        timestamp = int(time.time() * 1000)

        s3_file_key = (
            f"stock-prices/year={year}/month={month}/day={day}/hour={hour}/"
            f"worker{worker_id}-part-{timestamp}.parquet"
        )
        s3_path = f"s3://{BUCKET_NAME}/{s3_file_key}"

        pq.write_table(table, s3_path, filesystem=fs)
        print(f"[Worker {worker_id}] Flushed {len(records)} → {s3_file_key}")
        return True

    except Exception as e:
        if attempt < max_attempts:
            sleep_time = 2**attempt + random.random()
            print(
                f"[Worker {worker_id}] Flush failed (attempt {attempt}), "
                f"retrying in {sleep_time:.1f}s… {e}"
            )
            time.sleep(sleep_time)
            return flush_to_s3(records, worker_id, attempt + 1, max_attempts)
        else:
            print(
                f"[Worker {worker_id}] Flush failed permanently after "
                f"{max_attempts} attempts: {e}"
            )
            return False


def run_consumer(worker_id):
    """
    Consume stock events from Kafka and flush batches to S3.

    Args:
        worker_id (int): ID of the consumer worker process.

    Returns:
        None

    Continuously consumes messages, buffers, and flushes to S3 when
    batch size or interval is reached.
    """
    """Each worker consumes from Kafka and flushes to S3"""
    c = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": "stock-price-consumers",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 5 * 1024 * 1024,
            "fetch.wait.max.ms": 200,
            "max.partition.fetch.bytes": 50 * 1024 * 1024,
        }
    )

    c.subscribe([TOPIC])

    batch = []
    last_flush_time = time.time()

    print(f"Worker {worker_id} started…")
    try:
        while True:
            msgs = c.consume(num_messages=5000, timeout=1.0)

            for msg in msgs:
                if msg.error():
                    continue
                try:
                    record = orjson.loads(msg.value())
                    batch.append(record)
                except Exception:
                    pass

            if (
                len(batch) >= BATCH_SIZE
                or (time.time() - last_flush_time) >= FLUSH_INTERVAL
            ):
                flush_batch = batch.copy()
                success = flush_to_s3(flush_batch, worker_id)
                if success:
                    c.commit(asynchronous=True)
                batch.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print(f"Stopping Worker {worker_id}…")
        if batch:
            success = flush_to_s3(batch, worker_id)
            if success:
                c.commit(asynchronous=True)
    finally:
        c.close()


if __name__ == "__main__":
    """
    Entry point for running multiple consumer processes.

    Spawns several consumer workers in parallel, each consuming from
    Kafka and flushing to S3.
    """
    mp.set_start_method("spawn")
    num_workers = 5

    processes = []
    print(
        f"Flushing to {BUCKET_NAME} once batch={BATCH_SIZE} or "
        f"{FLUSH_INTERVAL}s interval."
    )
    for wid in range(num_workers):
        p = mp.Process(target=run_consumer, args=(wid,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
