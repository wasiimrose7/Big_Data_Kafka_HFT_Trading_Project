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
OUTPUT_CSV = "burst_metrics.csv"

# Setting up the CSV file for logging the Kafka metrics

def init_csv():
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "msg_id", "price", "mode", "burst",
            "produce_time", "consume_time",
            "latency_ms", "throughput_msg_per_sec",
            "p95_latency_ms", "p99_latency_ms"
        ])

# Metrics tracking function - This was created with the help of claude AI for computing various metrics for the data pipeline

class MetricsTracker:
    def __init__(self):
        self.latencies = []
        self.burst_latencies = []
        self.normal_latencies = []
        self.start_time = None
        self.msg_count = 0
        self.burst_count = 0

    def record(self, produce_ts, consume_ts, is_burst):
        if self.start_time is None:
            self.start_time = consume_ts

        latency_ms = (consume_ts - produce_ts) * 1000
        self.latencies.append(latency_ms)
        self.msg_count += 1

        if is_burst:
            self.burst_latencies.append(latency_ms)
            self.burst_count += 1
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
        "group.id": "hft-burst-group",   # ✅ separate group from baseline
        "enable.auto.commit": True
    })

    consumer = Consumer(config)
    seek_to_end(consumer, TOPIC)

    init_csv()
    tracker = MetricsTracker()

    print(f"Consuming from {TOPIC}... writing to {OUTPUT_CSV}")
    print("Waiting for burst producer. Start burst_producer.py now.\n")

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
                is_burst = data.get("burst", False)
                latency_ms, throughput, p95, p99 = tracker.record(
                    produce_ts, consume_ts, is_burst
                )

                writer.writerow([
                    data["id"],
                    data["price"],
                    data.get("mode", "unknown"),
                    is_burst,
                    produce_ts,
                    consume_ts,
                    round(latency_ms, 3),
                    round(throughput, 2),
                    round(p95, 3) if p95 else "",
                    round(p99, 3) if p99 else ""
                ])
                f.flush()

                if tracker.msg_count % 100 == 0:
                    burst_pct = (tracker.burst_count / tracker.msg_count) * 100
                    avg_burst_lat = (
                        round(np.mean(tracker.burst_latencies), 1)
                        if tracker.burst_latencies else 0
                    )
                    avg_normal_lat = (
                        round(np.mean(tracker.normal_latencies), 1)
                        if tracker.normal_latencies else 0
                    )
                    print(
                        f"Msg {tracker.msg_count} | "
                        f"Burst%: {burst_pct:.0f}% | "
                        f"Burst lat: {avg_burst_lat}ms | "
                        f"Normal lat: {avg_normal_lat}ms | "
                        f"P95: {f'{p95:.1f}ms' if p95 else 'building...'}"
                    )

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        print(f"\n--- Burst Experiment Summary ---")
        print(f"Total messages:  {tracker.msg_count}")
        print(f"Burst messages:  {tracker.burst_count} "
              f"({tracker.burst_count/max(tracker.msg_count,1)*100:.1f}%)")
        if tracker.burst_latencies:
            print(f"Avg burst latency:  {np.mean(tracker.burst_latencies):.1f}ms")
        if tracker.normal_latencies:
            print(f"Avg normal latency: {np.mean(tracker.normal_latencies):.1f}ms")
    finally:
        consumer.close()
        print("Done. CSV saved to burst_metrics.csv")

#Run

if __name__ == "__main__":
    run_consumer()
