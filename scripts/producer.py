from confluent_kafka import Producer
from create_topic import create_topic
import json,random,time


BROKERS='localhost:9092,localhost:9093,localhost:9094'
BATCH_SIZE=1000
sent_messages=0
start_time=time.time()

p = Producer({
    'bootstrap.servers': BROKERS,
    'compression.type':'lz4',
    'linger.ms':50,
    'queue.buffering.max.messages':100000,
})
stocks=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']


def delivery_report(err, msg):
    global sent_messages
    if err:
        print(f"Message delivery failed: {err}")
    else:
        sent_messages+= 1


if __name__ == "__main__":
    create_topic("stock-prices", num_partitions=12, replication_factor=3)
    try:
        while True:
            for _ in range(BATCH_SIZE):
                data = {"stock": random.choice(stocks),"price": round(random.uniform(100, 1500), 2)}
                p.produce("stock-prices",json.dumps(data).encode('utf-8'),callback=delivery_report)
            p.flush()

            elapsed_time=time.time()-start_time
            if elapsed_time > 0:
                mps = sent_messages / elapsed_time
                print(f"Throughput: {mps:.2f} msgs/sec")
            sent_messages=0
            start_time=time.time()
    except KeyboardInterrupt:
        print("Shutting down producer.")
        p.flush()
