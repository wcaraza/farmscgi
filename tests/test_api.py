from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_create_cow():
    response = client.post("/cows/", json={
        "name": "Bessie #999",
        "birthdate": "2020-01-01"
    })
    assert response.status_code == 200

def test_dummy():
    assert 1 == 1