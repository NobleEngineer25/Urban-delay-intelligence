from datetime import datetime
from math import asin, cos, radians, sin, sqrt

from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="Urban Delay Intelligence", version="0.1.0")


class TripRequest(BaseModel):
    """What the caller sends in."""

    origin_lat: float = Field(..., ge=-90, le=90, examples=[41.8781])
    origin_lon: float = Field(..., ge=-180, le=180, examples=[-87.6298])
    dest_lat: float = Field(..., ge=-90, le=90, examples=[41.9742])
    dest_lon: float = Field(..., ge=-180, le=180, examples=[-87.9073])
    departure_time: datetime = Field(..., examples=["2023-01-15T08:30:00"])


class TripPrediction(BaseModel):
    """What we send back. Same shape the real model will use later."""

    distance_km: float
    p50_seconds: int
    p80_seconds: int
    p90_seconds: int
    model_version: str


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Straight-line distance between two points on Earth, in km."""
    earth_radius_km = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * earth_radius_km * asin(sqrt(a))


@app.get("/health")
def health() -> dict:
    """Simple check that the service is alive. Docker and Cloud Run use this later."""
    return {"status": "ok"}


@app.post("/predict", response_model=TripPrediction)
def predict(trip: TripRequest) -> TripPrediction:
    """STUB prediction. Replaced by the real model in a later phase."""
    distance_km = haversine_km(
        trip.origin_lat, trip.origin_lon, trip.dest_lat, trip.dest_lon
    )

    # Fake formula: 2 min base + 140 s per km.
    # Tuned so a typical Chicago trip lands near the real 1,099 s average.
    p50 = int(120 + distance_km * 140)

    return TripPrediction(
        distance_km=round(distance_km, 2),
        p50_seconds=p50,
        p80_seconds=int(p50 * 1.3),
        p90_seconds=int(p50 * 1.6),
        model_version="stub-0.1",
    )