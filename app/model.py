from app.db import get_connection
from datetime import datetime, timedelta
import uuid

def create_cow(cow):
    conn = get_connection()

    cow_id = str(uuid.uuid4())

    conn.execute("""
        INSERT INTO cows (id, name, birthdate) VALUES (?, ?, ?)
    """, [
        cow_id,
        cow.name,
        cow.birthdate
    ])
    conn.close()


# def insert_sensor_data(data):
#     conn = get_connection()

#     conn.execute("""
#         INSERT INTO sensor_data (cow_id, timestamp, weight, milk)
#         VALUES (?, ?, ?, ?)
#     """, [data.cow_id, data.timestamp, data.weight, data.milk])

#     conn.close()


def get_cow(cow_id: uuid.UUID):
    conn = get_connection()

    cow = conn.execute(
        "SELECT * FROM cows WHERE id = ?", [cow_id]
    ).fetchone()

    latest = conn.execute("""
        SELECT * FROM measurements
        WHERE cow_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, [cow_id]).fetchone()

    conn.close()

    return cow, latest

def load_parquet():
    conn = get_connection()

    # 🐄 Cows
    conn.execute("""
        INSERT INTO cows
        SELECT id, name, birthdate FROM read_parquet('data/cows.parquet')
    """)

    # 🥛 Milk
    conn.execute("""
        INSERT INTO measurements
        SELECT sensor_id, cow_id, TO_TIMESTAMP(timestamp) as timestamp, value
        FROM read_parquet('data/measurements.parquet')
    """)

    # ⚖️ Weight
    conn.execute("""
        INSERT INTO sensors
        SELECT *
        FROM read_parquet('data/sensors.parquet')
    """)

    conn.close()