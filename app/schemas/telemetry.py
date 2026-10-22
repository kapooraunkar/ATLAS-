from pydantic import BaseModel, Field

class Telemetry(BaseModel):
    request_id: str
    method: str
    path: str
    status_code: int
    duration_ms: float
    cpu_percent: float
    memory_percent: float
    source: str = Field(default="atlas",min_length=1,max_length=100)