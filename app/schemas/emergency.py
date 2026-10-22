from pydantic import BaseModel
from typing import Optional
from enum import Enum

class EmergencyLevel(str,Enum):
    HIGH = "high"
    CRITICAL = "critical"

class EmergencyRequest(BaseModel):
    request_id: str
    reason: str
    level: EmergencyLevel

    created_at: float
    source_worker: Optional[str] = None
    requires_consensus: bool = True

