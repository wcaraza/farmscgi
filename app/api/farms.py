from fastapi import APIRouter
from app.schemas import CowCreate, MeasurementsCreate
from app.model import create_cow, get_cow, batch_load_parquet, insert_sensor_data, generate_report, insert_sensor_data_parquet, simulate_parallel
import uuid


router = APIRouter()

@router.post("/cows/")
def create_cow_endpoint(cow: CowCreate):
    create_cow(cow)
    return {"message": "Cow created"}


@router.post("/sensor-data")
def sensor_data_endpoint(measurements: MeasurementsCreate):
    #data.validate_data()
    insert_sensor_data(measurements)
    return {"message": "Sensor data inserted"}


@router.get("/cows/{id}")
def get_cow_endpoint(id: uuid.UUID):
    cow, latest = get_cow(id)
    return {
        "cow": cow,
        "latest_sensor_data": latest
    }

@router.get("/ingestion")
def get_ingestion_endpoint():
    batch_load_parquet()
    return {
        "Ingestion": "Parquet files loaded into the database"
    }

@router.get("/simulate-ingestion")
def get_simulate_ingestion_endpoint():
    #insert_sensor_data_parquet()
    simulate_parallel()
    return {
        "Ingestion": "Sensor data from parquet files loaded into the database"
    }

@router.get("/report/{report_date}")
def get_report_endpoint(report_date: str):
    report = generate_report(report_date)
    
    #print(report)

    with open(f"reports/report_{report_date}.txt", "w") as f:
        f.write(report)

    return {
        "Report": "Report generated successfully"
    }

