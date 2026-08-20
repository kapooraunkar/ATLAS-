from fastapi import FastAPI
from app.middleware.telemetry import telemetry
from app.database.init_db import initialize_database
from app.services.telemetry_worker import start_telemetry_workers

app = FastAPI()
initialize_database()
start_telemetry_workers()
app.middleware("http")(telemetry)

@app.get("/")
def root():
    return {"message" : "atlas is running"}

@app.get("/crash")
def crash():
    raise Exception("Atlas intentionally crashed")

@app.get("/test")
def test():
    return {"message": "test"}
