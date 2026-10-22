from enum import Enum

from app.services.telemetry_queue import BackpressureLevel


class BackpressureAction(Enum):
    NONE = "none"
    SHED_NORMAL = "shed_normal"
    SHED_NON_CRITICAL = "shed_non_critical"
    EMERGENCY = "emergency"

class BackpressurePolicy:

    def decide(self,level: BackpressureLevel) -> BackpressureAction:

        if level == BackpressureLevel.NONE:
            return BackpressureAction.NONE
        if level == BackpressureLevel.ELEVATED:
            return BackpressureAction.NONE
        if level == BackpressureLevel.HIGH:
            return BackpressureAction.SHED_NORMAL
        if level == BackpressureLevel.SEVERE:
            return BackpressureAction.SHED_NON_CRITICAL

        return BackpressureAction.NONE

backpressure_policy = BackpressurePolicy()