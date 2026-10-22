from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException,Query
from app.middleware.telemetry import telemetry
from app.database.init_db import initialize_database
from app.services.telemetry_worker import start_telemetry_workers,stop_telemetry_workers
from app.services.control_loop import control_loop
from app.services.metrics import get_internal_metrics
from app.services.health import get_liveliness, get_readiness
from fastapi.responses import Response
from app.services.prometheus_metrics import refresh_prometheus_metrics
from app.services.telemetry_queue import add_to_queue
from app.schemas.telemetry import Telemetry
from app.services.telemetry_query import get_telemetry_records

@asynccontextmanager
async def lifespan(app: FastAPI):

    initialize_database()

    start_telemetry_workers()

    control_loop.start()

    yield

    control_loop.stop()

    stop_telemetry_workers()
    
app = FastAPI(lifespan=lifespan)

app.middleware("http")(telemetry)


@app.get("/")
def root():
    return {"message" : "atlas is running"}

@app.get("/health")
def health():
    return get_liveliness()

@app.get("/ready")
def ready():
    return get_readiness()

@app.get("/internal/metrics")
def internal_metrics():
    return get_internal_metrics()

@app.get("/crash")
def crash():
    raise Exception("Atlas intentionally crashed")

@app.get("/test")
def test():
    return {"message": "test"}

@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    content, content_type = refresh_prometheus_metrics()

    return Response(
        content=content,
        media_type=content_type,
    )

@app.post("/ingest", status_code=202)
def ingest_telemetry(telemetry: Telemetry):
    accepted = add_to_queue(telemetry)

    if not accepted:
        raise HTTPException(
            status_code=503,
            detail="ATLAS could not accept telemetry at this time",
        )

    return {
        "message": "Telemetry accepted",
        "request_id": telemetry.request_id,
        "source": telemetry.source,
    }

@app.get("/telemetry")
def query_telemetry(
    source: str | None = Query(default=None, min_length=1,max_length=100),
    limit: int = Query(default=100,ge=1,le=500),
):
    records = get_telemetry_records(source=source, limit=limit)

    return {
        "count": len(records),
        "items": records,
    }