from confluent_kafka import Consumer
import orjson, time, os, multiprocessing as mp

BROKERS = 'localhost:8097,localhost:8098,localhost:8099'

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