from kafka import KafkaProducer
import time
import json
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=5000)      
parser.add_argument("--duration", type=int, default=30)    
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers="20.106.18.72:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=0,
    batch_size=16384
)

start = time.time()
i = 0

#for bursty effects in streaming
burst_base = max(50, args.rate // 20)
burst_jitter = int(burst_base * 0.5)
pause_base = 0.05

while time.time() - start < args.duration:

    burst_size = burst_base + random.randint(-burst_jitter, burst_jitter)
    burst_size = max(1, burst_size)

    for _ in range(burst_size):
        msg = {
            "id": i,
            "timestamp": time.time_ns()
        }

        producer.send("hft-test", msg)
        i += 1

    producer.flush()

    time.sleep(pause_base + random.uniform(0, 0.03))

producer.flush()
print(f"Done. Sent {i}")