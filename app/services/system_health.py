from enum import Enum
from threading import RLock

class HealthEvent(Enum):
    PERSISTENCE_FAILURE = "persistence_failure"
    PERSISTENCE_RECOVERED = "persistence_recovered"
    SYSTEM_EMERGENCY = "system_emergency"


class SystemHealthBus:

    def __init__(self):
        self.subscribers = []
        self.lock = RLock()

    def subscribe(self,callback):
        with self.lock:
            self.subscribers.append(callback)

    def publish(self,event):

        with self.lock:
            subscribers = self.subscribers.copy()

        for callback in subscribers:
            callback(event)

system_health_bus = SystemHealthBus()


def test_health_event(event):
    print(f"Recieved health event: {event.value}")
system_health_bus.subscribe(test_health_event)
system_health_bus.publish(HealthEvent.PERSISTENCE_FAILURE)

