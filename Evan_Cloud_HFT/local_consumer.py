from kafka import KafkaConsumer
import json
import time
import csv

#this is a local version of the consumer, mainly used for testing 

consumer = KafkaConsumer(
    "hft-test",
    bootstrap_servers='20.106.18.72:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("Consumer started")

with open("cloud_log.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["msg_id", "latency_ms", "received_time"])

    for msg in consumer:
        data = json.loads(msg.value)

        #records latency for measuring performance 
        latency = (time.time_ns() - data["timestamp"]) / 1e6
        received = time.time()

        writer.writerow([data["id"], latency, received])
        f.flush()

        print(f"{data['id']} -> {latency:.2f} ms")