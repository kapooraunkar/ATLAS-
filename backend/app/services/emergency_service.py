from app.schemas.emergency import EmergencyRequest,EmergencyLevel
import time

class EmergencyService:

    def should_escalate(self, telemetry) -> bool:
        return False

    def create_request(
        self,
        telemetry,
        reason: str,
        level: EmergencyLevel,
        source_worker: str | None = None) -> EmergencyRequest:

        return EmergencyRequest(
            request_id=telemetry.request_id,
            reason=reason,
            level=level,
            created_at=time.monotonic(),
            source_worker=source_worker,
            requires_consensus=True
        )

emergency_service = EmergencyService()
