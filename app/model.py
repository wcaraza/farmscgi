from app.db import get_connection
from datetime import datetime, timedelta
import uuid
import pandas as pd
import requests
import math



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


def insert_sensor_data(data):
    conn = get_connection()

    conn.execute("""
        INSERT INTO measurements (sensor_id, cow_id, timestamp, value)
        VALUES (?, ?, ?, ?)
    """, [
        data.sensor_id, 
        data.cow_id, 
        data.timestamp, 
        data.value
        ])
    conn.close()


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

def batch_load_parquet():
    conn = get_connection()

    #cows dataset
    conn.execute("""
        INSERT INTO cows
        SELECT id, name, birthdate FROM read_parquet('data/cows.parquet')
    """)

    #measurements dataset
    conn.execute("""
        INSERT INTO measurements
        SELECT sensor_id, cow_id, TO_TIMESTAMP(timestamp) as timestamp, value
        FROM read_parquet('data/measurements.parquet')
    """)

    #sensors dataset
    conn.execute("""
        INSERT INTO sensors
        SELECT *
        FROM read_parquet('data/sensors.parquet')
    """)

    conn.close()

def generate_report(date: str):
    conn = get_connection()

    report_date = datetime.fromisoformat(date)

    # Milk per cow per day
    milk = conn.execute("""
        SELECT 
        m.cow_id,
        c.name, 
        s.unit, 
        STRFTIME(m.timestamp, '%Y-%m-%d'), 
        SUM(m.value) as totalmilk
        FROM sensors s
        INNER JOIN measurements m ON m.sensor_id=s.id
        INNER JOIN cows c ON c.id=m.cow_id
        WHERE
        s.unit='L'
        and STRFTIME(m.timestamp, '%Y-%m-%d') = ?
        group by m.cow_id,c.name,s.unit,STRFTIME(m.timestamp, '%Y-%m-%d')
    """, [report_date]).fetchall()

    # Weight stats
    weight = conn.execute("""
        SELECT *
        FROM (
            SELECT 
                cow_id,
                c.name,
                s.unit,
                AVG(value) FILTER (
                    WHERE CAST(timestamp AS TIMESTAMP) >= (
                        --SELECT CAST(MAX(timestamp) AS TIMESTAMP)- INTERVAL 30 DAY FROM measurements
                        SELECT CAST(MAX('2024-07-11 05:00:00.000') AS TIMESTAMP)- INTERVAL 30 DAY FROM measurements
                    ) 
                ) OVER (PARTITION BY cow_id) AS avg_weight_30d,
                ROW_NUMBER() OVER (
                    PARTITION BY cow_id 
                    ORDER BY timestamp DESC
                ) AS rn,
                case 
                    when value <= 0 then avg_weight_30d
                    else value
                end as current_weight
            FROM measurements m
            INNER JOIN sensors s ON s.id=m.sensor_id
            INNER JOIN cows c ON c.id=m.cow_id
            WHERE s.unit='kg'
        ) t
        WHERE rn = 1;
    """).fetchall()

    ills = conn.execute("""
        SELECT cow_id,
            (select AVG(value) as avgmilk from measurements m inner join sensors s ON s.id=m.sensor_id where s.unit='L' and mp.cow_id=m.cow_id group by cow_id),
            (select AVG(value) as avgweight from measurements m inner join sensors s ON s.id=m.sensor_id where s.unit='kg' and mp.cow_id=m.cow_id group by cow_id)        
        FROM measurements mp
        WHERE timestamp >= CAST('2024-07-11 05:00:00.000' AS TIMESTAMP) - INTERVAL 7 DAY
        GROUP BY cow_id;
    """).fetchall()

    conn.close()

    report = f"Report for {date}\n\nMilk Production:\n"
    for row in milk:
        report += f"Cow {row[1]}: {row[4]} L\n"

    report += "\nWeight:\n"
    for row in weight:
        report += f"Cow {row[1]}: Current {row[5]}, Avg30d {row[3]}\n"

    report += "\nCows under threshold:\n"
    for row in ills:
        if row[1] < 4.7:  # limit for milk production
            report += f"Cow {row[0]}: Avg Milk {row[1]}\n"

    return report

def clean(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    return v

#Sequential ingestion of data from parquet files, not recommended for large datasets
def insert_sensor_data_parquet():

    df = pd.read_parquet('data/measurements.parquet')
    df = df.where(df.notnull(), None)

    for _, row in df.iterrows():
        record = {
            "sensor_id": clean(row["sensor_id"]),
            "cow_id": clean(row["cow_id"]),
            "timestamp": clean(row["timestamp"]),
            "value": clean(row["value"])
        }
        requests.post("http://localhost:8000/sensor-data", json=record)


def send_row(row):
    payload = {
        "sensor_id": clean(row["sensor_id"]),
            "cow_id": clean(row["cow_id"]),
            "timestamp": clean(row["timestamp"]),
            "value": clean(row["value"])
    }

    requests.post("http://localhost:8000/sensor-data", json=payload)


#Parallel ingestion of data from parquet files using ThreadPoolExecutor, recommended for large datasets
from concurrent.futures import ThreadPoolExecutor
def simulate_parallel():
    df = pd.read_parquet('data/measurements.parquet')

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(send_row, [row for _, row in df.iterrows()])