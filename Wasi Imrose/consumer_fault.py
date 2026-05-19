import json
import time
import csv
import os
import numpy as np
from confluent_kafka import Consumer, TopicPartition, OFFSET_END

# Configuration set up for Kafka Producer - This was created using the Confluent Kafka Documentation

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.split("=", 1)
                config[parameter] = value.strip()
    return config

TOPIC = "topic_1"
OUTPUT_CSV = "fault_metrics.csv"

# Setting up the CSV file for logging the Kafka metrics

def init_csv():
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "msg_id", "price", "mode", "burst", "fault",
            "produce_time", "consume_time",
            "latency_ms", "throughput_msg_per_sec",
            "p95_latency_ms", "p99_latency_ms"
        ])

# Metrics tracking function - This was created with the help of claude AI for computing various metrics for the data pipeline

class MetricsTracker:
    def __init__(self):
        self.latencies = []
        self.fault_latencies = []
        self.normal_latencies = []
        self.start_time = None
        self.msg_count = 0
        self.fault_count = 0

    def record(self, produce_ts, consume_ts, is_fault):
        if self.start_time is None:
            self.start_time = consume_ts

        latency_ms = (consume_ts - produce_ts) * 1000
        self.latencies.append(latency_ms)
        self.msg_count += 1

        if is_fault:
            self.fault_latencies.append(latency_ms)
            self.fault_count += 1
        else:
            self.normal_latencies.append(latency_ms)

        elapsed = consume_ts - self.start_time
        throughput = self.msg_count / max(elapsed, 0.001)

        p95 = float(np.percentile(self.latencies, 95)) if len(self.latencies) >= 20 else None
        p99 = float(np.percentile(self.latencies, 99)) if len(self.latencies) >= 100 else None

        return latency_ms, throughput, p95, p99

# Skip stale

def seek_to_end(consumer, topic):
    metadata = consumer.list_topics(topic)
    partitions = [
        TopicPartition(topic, p, OFFSET_END)
        for p in metadata.topics[topic].partitions.keys()
    ]
    consumer.assign(partitions)
    print(f"Seeked to end of {len(partitions)} partition(s). Only new messages will be read.\n")

# Consumer Loop - This was created using a mix of Confluent and Python dosumentation and Clause AI for debugging

def run_consumer():
    config = read_config()
    config.update({
        "group.id": "hft-fault-group",    # separate group
        "enable.auto.commit": True
    })

    consumer = Consumer(config)
    seek_to_end(consumer, TOPIC)

    init_csv()
    tracker = MetricsTracker()

    print(f"Consuming from {TOPIC}... writing to {OUTPUT_CSV}")
    print("Waiting for fault producer. Start fault_producer.py now.\n")

    try:
        with open(OUTPUT_CSV, "a", newline="") as f:
            writer = csv.writer(f)

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    print("Waiting for messages...", end="\r")
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                consume_ts = time.time()

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    continue

                if "id" not in data or "timestamp" not in data:
                    continue

                produce_ts = data["timestamp"]
                is_fault = data.get("fault", False)
                latency_ms, throughput, p95, p99 = tracker.record(
                    produce_ts, consume_ts, is_fault
                )

                writer.writerow([
                    data["id"],
                    data["price"],
                    data.get("mode", "unknown"),
                    data.get("burst", False),
                    is_fault,
                    produce_ts,
                    consume_ts,
                    round(latency_ms, 3),
                    round(throughput, 2),
                    round(p95, 3) if p95 else "",
                    round(p99, 3) if p99 else ""
                ])
                f.flush()

                if tracker.msg_count % 100 == 0:
                    fault_pct = (tracker.fault_count / tracker.msg_count) * 100
                    avg_fault_lat = (
                        round(np.mean(tracker.fault_latencies), 1)
                        if tracker.fault_latencies else 0
                    )
                    avg_normal_lat = (
                        round(np.mean(tracker.normal_latencies), 1)
                        if tracker.normal_latencies else 0
                    )
                    print(
                        f"Msg {tracker.msg_count} | "
                        f"Faults: {tracker.fault_count} ({fault_pct:.0f}%) | "
                        f"Fault lat: {avg_fault_lat}ms | "
                        f"Normal lat: {avg_normal_lat}ms | "
                        f"P95: {f'{p95:.1f}ms' if p95 else 'building...'}"
                    )

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        print(f"\n--- Fault Injection Summary ---")
        print(f"Total messages:   {tracker.msg_count}")
        print(f"Fault messages:   {tracker.fault_count} "
              f"({tracker.fault_count/max(tracker.msg_count,1)*100:.1f}%)")
        if tracker.fault_latencies:
            print(f"Avg fault latency:  {np.mean(tracker.fault_latencies):.1f}ms")
        if tracker.normal_latencies:
            print(f"Avg normal latency: {np.mean(tracker.normal_latencies):.1f}ms")
    finally:
        consumer.close()
        print("Done. CSV saved to fault_metrics.csv")

#Run

if __name__ == "__main__":
    run_consumer()
