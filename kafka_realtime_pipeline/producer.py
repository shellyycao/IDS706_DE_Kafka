import json
import random
import string
import time
from datetime import datetime, timedelta, timezone
import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker



CITIES = ["Nashville", "Seattle", "New York", "Chicago"]
PAYMENT_METHODS = ["card", "cash", "wallet"]

def random_id(prefix, length=6):
    return prefix + "_" + "".join(random.choices(string.digits, k=length))

def make_fake_ride():
    start = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 30))
    duration_min = random.uniform(5, 30)
    end = start + timedelta(minutes=duration_min)

    distance = round(duration_min * random.uniform(0.3, 0.6), 2)
    base_fare = 2.5 + distance * random.uniform(0.8, 1.4)
    surge = random.choice([1.0, 1.0, 1.0, 1.2, 1.5])  # more likely 1.0
    fare = round(base_fare * surge, 2)

    return {
        "ride_id": random_id("r"),
        "driver_id": random_id("d"),
        "rider_id": random_id("u"),
        "city": random.choice(CITIES),
        "status": "completed",
        "start_ts": start.isoformat(),
        "end_ts": end.isoformat(),
        "distance_km": distance,
        "fare_usd": fare,
        "surge_multiplier": surge,
        "payment_method": random.choice(PAYMENT_METHODS),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic = "rides"

    print("Starting ride producer...")
    while True:
        event = make_fake_ride()
        producer.send(topic, value=event)
        print("Sent:", event)
        # control rate: e.g. 2 events per second
        time.sleep(0.5)

if __name__ == "__main__":
    main()
