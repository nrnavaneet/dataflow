from confluent_kafka import Consumer
import json,time

BROKERS='localhost:9092,localhost:9093,localhost:9094'


c = Consumer({
    'bootstrap.servers': BROKERS,
    'group.id': 'stock-price-consumers',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'fetch.min.bytes': 1024*1024,
    'fetch.wait.max.ms': 100
})

c.subscribe(['stock-prices'])

print("Consuming messages from 'stock-prices' topic...")

count=0
start_time=time.time()

try:
    while True:
        msgs = c.consume(num_messages=500, timeout=1.0)
        for msg in msgs:
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            data = json.loads(msg.value().decode('utf-8'))
            count+=1
        
        elapsed_time=time.time()-start_time
        if elapsed_time >=1:
            mps = count / elapsed_time
            print(f"Consumed {count} messages. Throughput: {mps:.2f} msgs/sec")
            count=0
            start_time=time.time()

except KeyboardInterrupt:
    pass
finally:
    c.close()