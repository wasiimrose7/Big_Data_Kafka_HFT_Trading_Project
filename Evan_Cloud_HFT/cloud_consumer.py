import csv
import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "hft-test",
    bootstrap_servers="20.106.18.72:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

with open("cloud_log.csv", "a", newline="") as f:
    writer = csv.writer(f)

    if f.tell() == 0:
        writer.writerow(["msg_id", "latency_ms", "producer_ts_ns", "vm_receive_ts_ns"])

    for msg in consumer:
        data = msg.value

        producer_ts = data["timestamp"]
        vm_receive_ts = time.time_ns()

        latency_ms = (vm_receive_ts - producer_ts) / 1e6

        writer.writerow([
            data["id"],
            latency_ms,
            producer_ts,
            vm_receive_ts
        ])

        f.flush()

        print(f"{data['id']} -> {latency_ms:.2f} ms")



