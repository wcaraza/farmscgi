# Farms

This project implements an API to automate data management for a dairy farm. 

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

FastAPI, DuckDB, Pydantic, pytest, pandas, Jupiter notebook
