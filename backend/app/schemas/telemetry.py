from pydantic import BaseModel

class Telemetry(BaseModel):
    request_id: str
    method: str
    path: str
    status_code: int
    duration_ms: float
    cpu_percent: float
    memory_percent: float