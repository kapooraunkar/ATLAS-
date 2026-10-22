from enum import Enum
from threading import RLock
import time 
import random

CIRCUIT_FAILURE_THRESHOLD = 3
CIRCUIT_RECOVERY_TIMEOUT = 10   

CIRCUIT_JITTER_SECONDS = 2

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:

    def __init__(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.opened_at = None
        self.half_open_probe_in_progress = False
        self.recovery_deadline = None
        self.lock = RLock()

    def get_state(self) -> CircuitState:
        with self.lock:
            return self.state 

    def open(self):
        with self.lock:
            self._change_state(CircuitState.OPEN)
            self.opened_at = time.monotonic()

            jitter = random.uniform(
                0,
                CIRCUIT_JITTER_SECONDS
            )

            self.recovery_deadline = (
                self.opened_at
                + CIRCUIT_RECOVERY_TIMEOUT
                + jitter
            
            )

    def half_open(self):
        with self.lock:
            self._change_state(CircuitState.HALF_OPEN)

    def close(self):
        with self.lock:
            self.half_open_probe_in_progress = False
            self._change_state(CircuitState.CLOSED)

    def record_failure(self):
        with self.lock:

            print(
            f"Circuit success recorded: "
            f"state={self.state.value.upper()}"
            )
            
            self.failure_count += 1
            self.half_open_probe_in_progress = False

            if self.failure_count >= CIRCUIT_FAILURE_THRESHOLD:
                self.open()

    def record_success(self):  # a succesfull attempt while half open restores the circuit 
        with self.lock:

            self.failure_count = 0
            self.half_open_probe_in_progress = False

            if self.state == CircuitState.HALF_OPEN:
                self._change_state(CircuitState.CLOSED)

    def allow_request(self) -> bool:
        with self.lock:

            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:

                if time.monotonic() >= self.recovery_deadline:
                    self._change_state(CircuitState.HALF_OPEN)
                    self.half_open_probe_in_progress = True
                    return True
                
                return False

            
            if self.state == CircuitState.HALF_OPEN:

                if self.half_open_probe_in_progress:
                    return False
                self.half_open_probe_in_progress = True
                return True

    def _change_state(self,new_state):

        if self.state != new_state:
            print(
                f"Circuit state changed: "
                f"{self.state.value.upper()} -> "
                f"{new_state.value.upper()}"
            )

        self.state = new_state

    def respond_to_system_failure(self):  #system announced that the dependency is unhealthy
        with self.lock:
            self.open()

    def respond_to_system_recovery(self):
        with self.lock:
            if self.state == CircuitState.OPEN:
                self._change_state(CircuitState.HALF_OPEN)
                self.half_open_probe_in_progress = False

                