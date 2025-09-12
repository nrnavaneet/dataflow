from confluent_kafka import Consumer
import json,time

BROKERS='localhost:8097,localhost:8098,localhost:8099'

c = Consumer({
    'bootstrap.servers': BROKERS,
    'group.id': 'stock-price-consumers',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'fetch.min.bytes': 1024*1024,
    'fetch.wait.max.ms': 100,
    'max.partition.fetch.bytes': 5*1024*1024,
    'max.poll.records': 2000,           # default 500, increase if your app can handle
    'queued.max.messages.kbytes': 102400, # 100 MB internal queue
    'queued.min.messages': 10000,         # ensure deep queue
    'session.timeout.ms': 45000,          # allow longer heartbeats
    'heartbeat.interval.ms': 15000,
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