from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
import uuid

class CowCreate(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str
    birthdate: datetime

class MeasurementsCreate(BaseModel):
    sensor_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    cow_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    timestamp: datetime
    value: float

class SensorData(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    unit: str