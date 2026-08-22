import time
from enum import Enum
from collections import deque

class PersistenceState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


HEALTH_WINDOW_SECONDS = 30

DEGRADED_FAILURE_RATE = 0.10
UNHEALTHY_FAILURE_RATE = 0.50

DEGRADED_LATENCY_SECONDS = 0.5
UNHEALTHY_LATENCY_SECONDS = 2.0

class PersistenceHealth:
    def __init__(self):
        self.attempts = deque()
        self.state = PersistenceState.HEALTHY

    def get_state(self) -> PersistenceState:
        return self.state

    def _remove_old_attempts(self):
        cutoff = time.monotonic() - HEALTH_WINDOW_SECONDS

        while(self.attempts and self.attempts[0][0] < cutoff):
            self.attempts.popleft()

    def record_attempt(self,
        success: bool,
        latency: float
    ):
        self.attempts.append((
            time.monotonic(),success,latency
        ))

        self._remove_old_attempts()
        self._update_state()


    def _update_state(self):

        metrics = self.get_metrics()

        failure_rate = metrics["failure_rate"]
        average_latency = metrics["average_latency"]

        previous_state = self.state

        if (failure_rate >= UNHEALTHY_FAILURE_RATE
            or average_latency >= UNHEALTHY_LATENCY_SECONDS):
            self.state = PersistenceState.UNHEALTHY

        elif (failure_rate >= DEGRADED_FAILURE_RATE
              or average_latency >= DEGRADED_LATENCY_SECONDS):

            self.state = PersistenceState.UNHEALTHY


        else:
            self.state = PersistenceState.HEALTHY

        if self.state != previous_state:

            print(
                f"Persistence state changed: "
                f"{previous_state.value.upper()} → "
                f"{self.state.value.upper()} "
                f"(failure rate: "
                f"{failure_rate * 100:.1f}%, "
                f"avg latency: "
                f"{average_latency:.3f}s)"
            )


    def get_metrics(self):

        total_attempts = len(self.attempts)

        if total_attempts == 0:
            return {
                "failure_rate": 0.0,
                "average_latency": 0.0,
                "attempts": 0
            }
        
        failures = sum(
            1
            for _,success, _ in self.attempts
            if not success)
        
        failure_rate = (failures / total_attempts)
        
        average_latency = (sum(
            latency
            for _, _, latency in self.attempts
            )/ total_attempts)

        return {
            "failure_rate": failure_rate,
            "average_latency": average_latency,
            "attempts": total_attempts
        }

persistence_health = PersistenceHealth()