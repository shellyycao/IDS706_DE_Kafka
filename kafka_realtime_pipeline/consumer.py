import json
from datetime import datetime

from kafka import KafkaConsumer
import psycopg2 

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rides"

PG_CONN_INFO = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kafka_db",
    "user": "kafka_user",
    "password": "kafka_password",
}

def parse_ts(ts_str):
    # handle ISO string with timezone
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ride-consumers",
    )

    conn = psycopg2.connect(**PG_CONN_INFO)
    conn.autocommit = True
    cur = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rides (
        ride_id          TEXT PRIMARY KEY,
        driver_id        TEXT,
        rider_id         TEXT,
        city             TEXT,
        status           TEXT,
        start_ts         TIMESTAMP,
        end_ts           TIMESTAMP,
        distance_km      DOUBLE PRECISION,
        fare_usd         DOUBLE PRECISION,
        surge_multiplier DOUBLE PRECISION,
        payment_method   TEXT,
        ingested_at      TIMESTAMP DEFAULT NOW()
    );
    """
    cur.execute(create_table_sql)

    insert_sql = """
        INSERT INTO rides (
            ride_id, driver_id, rider_id, city, status,
            start_ts, end_ts, distance_km, fare_usd,
            surge_multiplier, payment_method
        )
        VALUES (%(ride_id)s, %(driver_id)s, %(rider_id)s, %(city)s, %(status)s,
                %(start_ts)s, %(end_ts)s, %(distance_km)s, %(fare_usd)s,
                %(surge_multiplier)s, %(payment_method)s)
        ON CONFLICT (ride_id) DO NOTHING;
    """

    print("Starting consumer...")
    for msg in consumer:
        data = msg.value
        try:
            data["start_ts"] = parse_ts(data["start_ts"])
            data["end_ts"] = parse_ts(data["end_ts"])
            cur.execute(insert_sql, data)
            print("Inserted ride", data["ride_id"])
        except Exception as e:
            print("Error inserting record:", e, data)

if __name__ == "__main__":
    main()
