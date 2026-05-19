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

TOPIC = "topic_2"
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

# Burst Simulator with Scaling - Similar code as Burst producer script with changed parameters (12 partitions)


def run_simulation(num_messages=1000, rate=200, burst_prob=0.6):
    price = 100.0
    start_time = time.time()

    print(f"Starting burst simulation (12 partitions): {num_messages} msgs at {rate} msg/s")
    print(f"Topic: {TOPIC} | Burst probability: {burst_prob*100:.0f}%\n")

    for i in range(num_messages):
        burst = random.random() < burst_prob
        price = generate_tick(price, burst)

        message = {
            "id": i,
            "price": price,
            "timestamp": time.time(),
            "burst": burst,
            "mode": "burst_12p"
        }

        producer.produce(
            TOPIC,
            value=json.dumps(message),
            callback=delivery_report
        )
        producer.poll(0)

        if i % 100 == 0:
            elapsed = time.time() - start_time
            print(f"Sent {i} msgs | {i/max(elapsed,0.001):.1f} msg/s")

        time.sleep(1 / rate)

    print("\nFlushing...")
    producer.flush(10)

    elapsed = time.time() - start_time
    print(f"Done. {num_messages} msgs in {elapsed:.1f}s "
          f"| Avg rate: {num_messages/elapsed:.1f} msg/s")

# Run - From Confluent Kafka documentation

if __name__ == "__main__":
    print("Burst 12P experiment starting in 5 seconds...")
    print("Switch to your consumer terminal and start burst_12p_consumer.py now.\n")
    time.sleep(5)
    run_simulation()
