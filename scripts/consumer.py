from confluent_kafka import Consumer
import json

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock-price-consumers',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['stock-prices'])

print("Consuming messages from 'stock-prices' topic...")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {data}")

except KeyboardInterrupt:
    pass
finally:
    c.close()