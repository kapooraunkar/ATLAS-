from threading import RLock
from app.services.system_health import (system_health_bus,HealthEvent)
from app.services.telemetry_queue import (
    BackpressureLevel,
    get_backpressure_level,
)

from app.services.backpressure_policy import (
    backpressure_policy,
    BackpressureAction,
)
from app.services.backpressure_executor import backpressure_executor



class BackpressureController:
    def __init__(self):
        self.current_level = BackpressureLevel.NONE
        self.current_action = BackpressureAction.NONE
        self.lock = RLock()

    def evaluate(self):
        level = get_backpressure_level()
        action = backpressure_policy.decide(level)

        with self.lock:

            previous_level = self.current_level
            previous_action = self.current_action

            previous_level = self.current_level
            self.current_level = level

        if level!= previous_level:
            print(
                f"Atlas backpressure changed: "
                f"{previous_level.value.upper()} -> "
                f"{level.value.upper()}"
            )

            self._publish_transition(level)

        if action != previous_action:
            print(
                f"Atlas backpressure action changed: "
                f"{previous_action.value.upper()} -> "
                f"{action.value.upper()}"
            )

        backpressure_executor.execute(action)

        return level, action

    def get_level(self):
        with self.lock:
            return self.current_level

    def _publish_transition(self, level):
        if level == BackpressureLevel.ELEVATED:
            system_health_bus.publish(HealthEvent.BACKPRESSURE_ELEVATED)

        elif level == BackpressureLevel.HIGH:
            system_health_bus.publish(HealthEvent.BACKPRESSURE_HIGH)

        elif level == BackpressureLevel.SEVERE:
            system_health_bus.publish(HealthEvent.BACKPRESSURE_SEVERE)

        elif level == BackpressureLevel.NONE:
            system_health_bus.publish(HealthEvent.BACKPRESSURE_RECOVERED)

    def get_action(self):
        level = self.get_level()
        return backpressure_policy.decide(level)

backpressure_controller = BackpressureController()