from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import uuid

class CowCreate(BaseModel):
    name: str
    birthdate: datetime

class MeasurementsCreate(BaseModel):
    sensor_id: uuid.UUID
    cow_id: uuid.UUID
    timestamp: datetime
    value: float

class SensorData(BaseModel):
    id: uuid.UUID
    unit: str