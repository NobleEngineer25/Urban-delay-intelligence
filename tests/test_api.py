from fastapi.testclient import TestClient

from serving.main import app

client = TestClient(app)

VALID_TRIP = {
    "origin_lat": 41.8781,
    "origin_lon": -87.6298,
    "dest_lat": 41.9742,
    "dest_lon": -87.9073,
    "departure_time": "2023-01-15T08:30:00",
}


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_predict_returns_ordered_intervals():
    response = client.post("/predict", json=VALID_TRIP)
    assert response.status_code == 200
    body = response.json()
    assert body["p50_seconds"] <= body["p80_seconds"] <= body["p90_seconds"]


def test_predict_rejects_invalid_latitude():
    bad_trip = {**VALID_TRIP, "origin_lat": 200}
    response = client.post("/predict", json=bad_trip)
    assert response.status_code == 422