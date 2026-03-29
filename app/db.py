import duckdb

DB_PATH = "data/cows.db"

def get_connection():
    return duckdb.connect(DB_PATH)

def init_db():
    conn = get_connection()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS cows (
        id UUID PRIMARY KEY,
        name varchar(255),
        birthdate DATE
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        sensor_id UUID,
        cow_id UUID,
        timestamp TIMESTAMP,
        value DOUBLE,
        PRIMARY KEY (sensor_id, cow_id, timestamp)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS sensors (
        id UUID PRIMARY KEY,
        unit varchar(10)
    )
    """)

    conn.close()