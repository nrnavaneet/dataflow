from confluent_kafka import Consumer
import orjson, time, os, multiprocessing as mp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv
from datetime import datetime
import threading

load_dotenv()
BROKERS = 'localhost:8097,localhost:8098,localhost:8099'
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")

fs = s3fs.S3FileSystem(
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    client_kwargs={"region_name": REGION}
)

def flush_to_s3(records):
    if not records:
        return
    
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    now = datetime.now()
    month_name = now.strftime("%b")        # e.g., Sep
    day_name = now.strftime("%a")          # e.g., Mon
    day_num = now.strftime("%d")           # e.g., 14
    hour_path = now.strftime("%H")         # e.g., 15
    timestamp = int(time.time() * 1000)    # unique file suffix

    s3_file_key = f"stock-prices/{month_name}/{day_name}-{day_num}/{hour_path}/stock-prices_{timestamp}.parquet"
    s3_path = f"s3://{BUCKET_NAME}/{s3_file_key}"

    pq.write_table(table, s3_path, filesystem=fs)
    print(f"Flushed {len(records)} records to {s3_file_key}")

# Shared dict for reporting counts across workers
def run_consumer(worker_id, counter_dict):
    c = Consumer({
        'bootstrap.servers': BROKERS,
        'group.id': 'stock-price-consumers',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'fetch.min.bytes': 5 * 1024 * 1024,
        'fetch.wait.max.ms': 200,
        'max.partition.fetch.bytes': 50 * 1024 * 1024,
    })

    c.subscribe(['stock-prices'])

    count = 0
    start_time = time.time()

    print(f"Worker {worker_id} started...")

    try:
        while True:
            msgs = c.consume(num_messages=2000, timeout=1.0)
            for msg in msgs:
                if msg.error():
                    continue
                try:
                    orjson.loads(msg.value())
                    count += 1
                except Exception:
                    pass

            elapsed_time = time.time() - start_time
            if elapsed_time >= 1:
                rate = count / elapsed_time
                counter_dict[worker_id] = rate
                #print(f"Worker {worker_id} → {rate:.2f} msgs/sec")
                count = 0
                start_time = time.time()

            c.commit(asynchronous=True)
    except KeyboardInterrupt:
        print(f"Stopping Worker {worker_id}...")
    finally:
        c.close()


def aggregator(counter_dict, num_workers):
    while True:
        time.sleep(1)
        total = sum(counter_dict.values())
        if total > 0:
            print(f"TOTAL → {total:.2f} msgs/sec consumed...")


if __name__ == "__main__":
    num_workers = 3
    with mp.Manager() as manager:
        counter_dict = manager.dict()

        processes = []
        for wid in range(num_workers):
            p = mp.Process(target=run_consumer, args=(wid, counter_dict))
            p.start()
            processes.append(p)

        agg = mp.Process(target=aggregator, args=(counter_dict, num_workers))
        agg.start()

        for p in processes:
            p.join()