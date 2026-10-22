import time
from threading import Event, RLock, Thread

from app.services.backpressure_controller import backpressure_controller


CONTROL_INTERVAL_SECONDS = 1

class ControlLoop:

    def __init__(self):
        self.stop_event = Event()
        self.thread = None
        self.lock = RLock()

    def _run(self):
        while not self.stop_event.is_set():
            try:
                backpressure_controller.evaluate()
            except Exception as e:
                print(f"Control loop evaluation failed: {e}")

            self.stop_event.wait(CONTROL_INTERVAL_SECONDS)

    def start(self):
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return

            self.stop_event.clear()

            self.thread = Thread(
                target=self._run,
                daemon=True,
                name="atlas-control-loop",
            )

            self.thread.start()

            print("Atlas control loop started")

    def stop(self):
        with self.lock:
            self.stop_event.set()

            thread = self.thread

            if thread is not None:
                thread.join(timeout=2)

            with self.lock:
                self.thread = None

            print("Atlas control loop stopped")


control_loop = ControlLoop()