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
        self.subscribers.append(callback)

    def publish(self,event):
        for callback in self.subscribers:
            callback(event)