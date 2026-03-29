from fastapi import APIRouter
from app.schemas import CowCreate, MeasurementsCreate
from app.model import create_cow, get_cow, load_parquet, insert_sensor_data
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
    load_parquet()
    return {
        "Ingestion": "Parquet files loaded into the database"
    }