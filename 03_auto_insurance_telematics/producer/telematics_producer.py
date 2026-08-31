"""
Telematics Event Producer

Simulates a small fleet of vehicles generating real-time driving behavior
events (normal driving, hard braking, rapid acceleration, speeding, harsh
cornering) and publishes them to a Kafka topic on Confluent Cloud.

Run this locally: python telematics_producer.py
Requires KAFKA_BOOTSTRAP_SERVER, KAFKA_API_KEY, KAFKA_API_SECRET set as
environment variables (never hardcoded - this script is safe to commit
to GitHub as-is).
"""

import os
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# --- Configuration, pulled from environment variables, never hardcoded ---
conf = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
}

TOPIC_NAME = "telematics_events"

# A small simulated fleet - 25 vehicles, matching the scale of a portfolio
# demo rather than a real production fleet of thousands.
VEHICLE_IDS = [f"VEH{str(i).zfill(4)}" for i in range(1, 26)]

# Weighted event types - normal driving should dominate, same way real
# driving behavior does; harsh events are rarer, matching reality.
EVENT_TYPES = [
    ("normal_driving", 70),
    ("hard_brake", 10),
    ("rapid_acceleration", 8),
    ("speeding", 8),
    ("harsh_cornering", 4),
]


def weighted_event_type():
    """Pick an event type according to the weights above, using random.choices."""
    types = [e[0] for e in EVENT_TYPES]
    weights = [e[1] for e in EVENT_TYPES]
    return random.choices(types, weights=weights, k=1)[0]


def generate_event(vehicle_id: str) -> dict:
    """Build one simulated telematics event for a given vehicle."""
    event_type = weighted_event_type()

    # Speed is correlated with event type - a "speeding" event should
    # plausibly show a higher speed than "normal_driving", not just a
    # random number unrelated to the label.
    if event_type == "speeding":
        speed_mph = round(random.uniform(70, 95), 1)
    elif event_type in ("hard_brake", "rapid_acceleration"):
        speed_mph = round(random.uniform(25, 55), 1)
    else:
        speed_mph = round(random.uniform(15, 65), 1)

    return {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "event_type": event_type,
        "speed_mph": speed_mph,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    """Called once for each produced message, confirming success or failure."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}]")


def main():
    producer = Producer(conf)
    print(f"Producing simulated telematics events to '{TOPIC_NAME}'. Press Ctrl+C to stop.")

    try:
        while True:
            vehicle_id = random.choice(VEHICLE_IDS)
            event = generate_event(vehicle_id)

            producer.produce(
                topic=TOPIC_NAME,
                key=vehicle_id,
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)  # triggers delivery callbacks for previous sends

            print(f"Sent: {event}")
            time.sleep(random.uniform(0.5, 2.0))  # simulate realistic event spacing

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()  # make sure any in-flight messages are sent before exiting


if __name__ == "__main__":
    main()