# FarmSCGI

Este proyecto implementa una API para automatizar la gestión de datos de una granja de vacas. Permite registrar vacas, recibir datos de sensores (peso y producción de leche) y generar reportes diarios.

## Features:

- API REST Python + FastAPI
- Data persistence with DuckDB
- Sensor data insertion
- Daily reports
- Data loading simulation
- Automated tests

## Dependencies

pip install -r requirements.txt

## Structure

app/ -> API principal
data/ -> datasets
reports/ -> reports
tests/ -> tests

## Endpoints

- POST /cows
- GET /cows/{id}
- GET /ingestion
- GET /report/{date}
- GET /simulate-ingestion

## Simulation

Using ThreadPoolExecutor to parallelize data sending.

## Reporting

- Daily milk production
- Current weight + 30-day average
- Detection of potentially sick cows

## Tests

pytest -v

## Best practices

- Data validation
- Strong typing
- Scalability

## Stack

FastAPI, DuckDB, Pydantic, pytest, pandas
