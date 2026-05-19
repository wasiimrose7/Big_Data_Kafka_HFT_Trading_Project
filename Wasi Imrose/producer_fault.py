import json
import time
import random
from confluent_kafka import Producer

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
producer = Producer(read_config())

# Delivery Callback

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

# Tick generating Function - From standard Python documentation

def generate_tick(price, burst=False):
    base_change = random.gauss(0, 0.02)
    jump = 0
    if random.random() < 0.05:
        jump = random.choice([-1, 1]) * random.uniform(0.2, 1.0)
    if burst:
        change = base_change * random.uniform(5, 10) + jump
    else:
        change = base_change + jump
    return max(1, round(price + change, 2))


# Fault Simulation
# Note: This function was created with the help of Claude Code 


def run_simulation(num_messages=1000, rate=50, fault_prob=0.05, fault_delay_ms=200):
    price = 100.0
    start_time = time.time()
    fault_count = 0

    print(f"Starting fault injection: {num_messages} msgs at {rate} msg/s")
    print(f"Fault probability: {fault_prob*100:.0f}% | Fault delay: {fault_delay_ms}ms\n")

    for i in range(num_messages):
        burst = random.random() < 0.1
        price = generate_tick(price, burst)

        fault_triggered = random.random() < fault_prob

        # capture timestamp BEFORE the fault delay
        message_time = time.time()

        if fault_triggered:
            fault_count += 1
            time.sleep(fault_delay_ms / 1000)

        message = {
            "id": i,
            "price": price,
            "timestamp": message_time,    #  pre-fault timestamp
            "burst": burst,
            "mode": "fault_injection",
            "fault": fault_triggered
        }

        producer.produce(
            TOPIC,
            value=json.dumps(message),
            callback=delivery_report
        )
        producer.poll(0)

        if i % 100 == 0:
            elapsed = time.time() - start_time
            print(f"Sent {i} msgs | {i/max(elapsed,0.001):.1f} msg/s | "
                  f"Faults so far: {fault_count}")

        time.sleep(1 / rate)

    print("\nFlushing...")
    producer.flush(10)

    elapsed = time.time() - start_time
    print(f"Done. {num_messages} msgs in {elapsed:.1f}s | "
          f"Total faults injected: {fault_count} "
          f"({fault_count/num_messages*100:.1f}%)")

# Run - From Confluent Kafka documentation

if __name__ == "__main__":
    print("Fault injection experiment starting in 5 seconds...")
    print("Switch to your consumer terminal and start fault_consumer.py now.\n")
    time.sleep(5)
    run_simulation()
