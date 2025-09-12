from confluent_kafka import Producer
from scripts.create_topic import create_topic
import json,random,time

create_topic("stock-prices", num_partitions=3, replication_factor=1)

p = Producer({'bootstrap.servers':'localhost:9092'})
stocks=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

while True:
    data = {"stock": random.choice(stocks),"price": round(random.uniform(100, 1500), 2)}
    p.produce("stock-prices",json.dumps(data).encode('utf-8'),callback=delivery_report)
    p.poll(0)
    time.sleep(0.1)