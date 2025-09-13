from confluent_kafka import Producer
from create_topic import create_topic
import orjson, random, time, multiprocessing, uuid

BROKERS = 'localhost:8097,localhost:8098,localhost:8099'
BATCH_SIZE = 1000


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")


def produce_messages(proc_id):
    p = Producer({
        'bootstrap.servers': BROKERS,
        'compression.type': 'lz4',
        'linger.ms': 50,
        'queue.buffering.max.messages': 100000,
    })

    sent_messages = 0
    start_time = time.time()

    while True:
        for _ in range(BATCH_SIZE):
            data = {
            "stock": random.choice(["AAPL", "TSLA", "MSFT", "AMZN", "GOOG", "NFLX", "TCS", "INFY", "RELIANCE", "HDFCBANK"]),
            "price": round(random.uniform(50, 3000), 2),                 
            "volume": random.randint(100, 50000),                      
            "timestamp": time.time(),
            "exchange": random.choice(["NYSE", "NASDAQ", "BSE", "NSE"]),
            "order_type": random.choice(["BUY", "SELL", "SHORT", "COVER"]),
            "trade_id": str(uuid.uuid4()),
            "trader": random.choice(["fund_A", "fund_B", "fund_C", "hedge_X", "retail", "HNI"]),
            "sector": random.choice(["Tech", "Finance", "Energy", "Healthcare", "Automobile", "Retail", "Telecom"
            ]),
            "currency": random.choice(["USD", "INR", "EUR", "GBP", "JPY", "AED"]),
            "order_source": random.choice(["MobileApp", "Web", "API", "BrokerDesk"]),
            "settlement_type": random.choice(["T+1", "T+2", "Instant"]),
            "rating": random.choice(["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"])
            }
            p.produce("stock-prices", orjson.dumps(data), callback=delivery_report)
            sent_messages += 1

        p.flush()

        elapsed_time = time.time() - start_time
        if elapsed_time > 0:
            mps = sent_messages / elapsed_time
            print(f"[Proc {proc_id}] Throughput: {mps:.2f} msgs/sec")
        sent_messages = 0
        start_time = time.time()


if __name__ == "__main__":
    create_topic("stock-prices", num_partitions=12, replication_factor=3)

    num_procs = 5
    processes = []
    for i in range(num_procs):
        p = multiprocessing.Process(target=produce_messages, args=(i,))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Shutting down all producers...")
        for p in processes:
            p.terminate()