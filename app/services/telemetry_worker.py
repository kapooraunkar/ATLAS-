import time
from threading import Thread, Lock, current_thread, Event
from collections import deque
from queue import Empty,Full
from enum import Enum

from app.services.telemetry_queue import (
    telemetry_queue,
    get_queue_usage_percent,
    record_processed,
    get_rates,
    get_queue_state,
    add_failed_telemetry,
    failed_telemetry_queue,
    requeue_failed_telemetry,
    move_to_dead_letter,
    MAX_RECOVERY_RETRIES

)
from app.services.system_health import system_health_bus,HealthEvent
from app.services.circuit_breaker import CircuitBreaker

from app.services.telemetry_service import process_telemetry,check_persistence_health
from app.services.persistence_health import persistence_health, PersistenceState

# =========================
# Worker configuration
# =========================

PERSISTENCE_PROBE_INTERVAL = 5

FAILED_RETRY_INTERVAL = 10

WORKER_COUNT = 2
RECOVERY_WORKER_COUNT = 2

HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 15

RECOVERY_HEARTBEAT_INTERVAL = 5
RECOVERY_WORKER_TIMEOUT = 15
RECOVERY_SUPERVISOR_CHECK_INTERVAL = 1

LIMITED_RECOVERY_DELAY = 2

SUPERVISOR_CHECK_INTERVAL = 1

MAX_RESPAWNS = 3
RESPAWN_WINDOW_SECONDS = 10

MAX_RECOVERY_RESPAWNS = 3
RECOVERY_RESPAWN_WINDOW_SECONDS = 10

RESTART_BACKOFF_SECONDS = 2

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1



class RecoveryMode(Enum):
    NORMAL = "normal"
    LIMITED = "limited"
    PAUSED = "paused"



# =========================
# Worker heartbeat state
# =========================

worker_heartbeats = {}
heartbeat_lock = Lock()

recovery_worker_heartbeats = {}
recovery_heartbeat_lock = Lock()

persistence_probe_lock = Lock()
last_persistence_probe = 0.0


# =========================
# Heartbeat
# =========================

def worker_heartbeat(worker_name, stop_event):

    while not stop_event.is_set():

        with heartbeat_lock:
            worker_heartbeats[worker_name] = time.monotonic()

        stop_event.wait(HEARTBEAT_INTERVAL)

    # Remove heartbeat when worker is stopping
    with heartbeat_lock:
        worker_heartbeats.pop(worker_name, None)

# =========================
# Recovery Worker Heartbeat
# =========================

def recovery_worker_heartbeat(worker_name,stop_event):

    while not stop_event.is_set():

        with recovery_heartbeat_lock:

            recovery_worker_heartbeats[worker_name] = time.monotonic()

        stop_event.wait(RECOVERY_HEARTBEAT_INTERVAL)

    with recovery_heartbeat_lock:

        recovery_worker_heartbeats.pop(worker_name,None)

def should_probe_persistence():

    global last_persistence_probe

    now = time.monotonic()

    with persistence_probe_lock:

        if now - last_persistence_probe < PERSISTENCE_PROBE_INTERVAL:
            return False

        last_persistence_probe = now
        return True

# =========================
# RECOVERY MODE
# =========================

def get_recovery_mode():
    state = persistence_health.get_state()

    if state == PersistenceState.HEALTHY:
        return RecoveryMode.NORMAL
    elif state == PersistenceState.DEGRADED:
        return RecoveryMode.LIMITED
    else:
        return RecoveryMode.PAUSED

# =========================
# Telemetry worker
# =========================

def telemetry_worker():
    
    worker_name = current_thread().name

    stop_heartbeat = Event()

    # Register worker immediately
    with heartbeat_lock:
        worker_heartbeats[worker_name] = time.monotonic()

    # Start heartbeat thread
    heartbeat = Thread(
        target=worker_heartbeat,
        args=(worker_name, stop_heartbeat),
        daemon=True,
        name=f"{worker_name}-heartbeat"
    )

    heartbeat.start()

    #Worker local circuit breaker
    circuit = CircuitBreaker()

    #Subscribe this worker to system health announcements
    health_handler = create_health_handler(circuit)
    system_health_bus.subscribe(health_handler)

    try:

        while True:

            _, _, telemetry = telemetry_queue.get()

            ingestion_rate, processing_rate = get_rates()

            print(
                f"Ingestion: {ingestion_rate:.2f}/sec | "
                f"Processing: {processing_rate:.2f}/sec | "
                f"Queue: {get_queue_usage_percent():.2f}%"
            )

            print(
                f"Telemetry queue usage: "
                f"{get_queue_usage_percent():.2f}%"
            )

            try:

                persisted = False

                for attempt in range(1, MAX_RETRIES + 1):

                    start_time = time.monotonic()

                    try:

                        process_telemetry(telemetry)

                        latency = (time.monotonic() - start_time)
                        persistence_health.record_persistence_attempt(
                            success=True,
                            latency=latency
                        )

                        record_processed()

                        get_queue_state()

                        print(f"Telemetry persisted successfully attempt {attempt}")

                        persisted = True
                        break

                    except Exception as e:

                        latency = (time.monotonic() - start_time)

                        persistence_health.record_persistence_attempt(
                            success=False,
                            latency=latency
                        )

                        print(
                            f"Telemetry persistence failed"
                            f"(attmept {attempt}/{MAX_RETRIES}) : {e}"
                        )

                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY_SECONDS)

                        else:
                            print(
                                f"Telemetry persistence failed for request {telemetry.request_id}")
                            
        #THIS IS AFTER ALL RETRY ATTEMPTS 
                if not persisted:
                    print(
                        f"Telemetry permanently failed "
                        f"for request {telemetry.request_id}"
                    )

                    add_failed_telemetry(telemetry)

            finally:

                telemetry_queue.task_done()

    finally:

        # Tell heartbeat thread to stop
        stop_heartbeat.set()

        # Remove worker heartbeat
        with heartbeat_lock:
            worker_heartbeats.pop(worker_name, None)


# =========================
# RECOVERY WORKER
# =========================

def failed_telemetry_worker():

    worker_name = current_thread().name

    stop_heartbeat = Event()

    with recovery_heartbeat_lock:
        recovery_worker_heartbeats[worker_name] = time.monotonic()

        heartbeat = Thread(
            target=recovery_worker_heartbeat,
            args=(
                worker_name,
                stop_heartbeat
            ),
            daemon=True,
            name=f"{worker_name}-heartbeat"
        )
        heartbeat.start()

        try:

            while True:

                next_retry_time, _, failed_item = (
                    failed_telemetry_queue.get()
                )

                now = time.monotonic()
                if now < next_retry_time:

                    time.sleep(next_retry_time-now)

                    now = time.monotonic()

                mode = get_recovery_mode()

# =========================
# PAUSED
# =========================

                if mode == RecoveryMode.PAUSED:

                    if should_probe_persistence():

                        success, latency = (
                            check_persistence_health()
                        )

                        persistence_health.record_probe_result(
                            success=success,
                            latency=latency
                        )

                    failed_item.next_retry_time = (
                        time.monotonic()
                        + FAILED_RETRY_INTERVAL
                    )

                    requeue_failed_telemetry(
                        failed_item
                    )

                    continue


                # =========================
                # LIMITED
                # =========================

                if mode == RecoveryMode.LIMITED:

                    print(
                        f"Persistence degraded. "
                        f"Delaying recovery for "
                        f"{LIMITED_RECOVERY_DELAY}s"
                    )

                    time.sleep(
                        LIMITED_RECOVERY_DELAY
                    )


                # =========================
                # NORMAL + LIMITED
                # =========================

                start_time = time.monotonic()

                try:

                    process_telemetry(
                        failed_item.telemetry
                    )

                    latency = (
                        time.monotonic()
                        - start_time
                    )

                    persistence_health.record_persistence_attempt(
                        success=True,
                        latency=latency
                    )

                    print(
                        f"Failed telemetry recovered successfully "
                        f"{failed_item.telemetry.request_id}"
                    )

                except Exception as e:

                    latency = (
                        time.monotonic()
                        - start_time
                    )

                    persistence_health.record_persistence_attempt(
                        success=False,
                        latency=latency
                    )

                    failed_item.retry_count += 1

                    if failed_item.retry_count >= MAX_RECOVERY_RETRIES:

                        move_to_dead_letter(
                            failed_item
                        )

                        continue

                    backoff = min(
                        60,
                        2 ** failed_item.retry_count
                    )

                    failed_item.next_retry_time = (
                        time.monotonic()
                        + backoff
                    )

                    print(
                        f"Recovery failed for "
                        f"{failed_item.telemetry.request_id}. "
                        f"Retry #{failed_item.retry_count} "
                        f"in {backoff}s: {e}"
                    )

                    requeue_failed_telemetry(
                        failed_item
                    )

                finally:

                    failed_telemetry_queue.task_done()

        finally:
            stop_heartbeat.set()

            with recovery_heartbeat_lock:
                recovery_worker_heartbeats.pop(worker_name,None)

# =========================
# Worker-side handler
# =========================
def create_health_handler(circuit):
    def handle_health_event(event):

        if event == HealthEvent.PERSISTENCE_FAILURE:
            circuit.respond_to_system_failure()
        elif event == HealthEvent.PERSISTENCE_RECOVERED:
            circuit.respond_to_system_recovery()
    return handle_health_event


# =========================
# HANDLER FACTORY
# =========================

def create_health_handler(circuit):

    def handle_health_event(event):

        print(f"Worker recieved health event: {event.value}")

        if event == HealthEvent.PERSISTENCE_FAILURE:
            print("Worker recieved persistence failure announcement")
            circuit.respond_to_system_failure()
            
        elif event == HealthEvent.PERSISTENCE_RECOVERED:
            print("Worker received persistence recovery announcement")
            circuit.respond_to_system_recovery()

    return handle_health_event
# =========================
# Recovery Worker supervisor
# =========================

def recovery_worker_supervisor():
    recovery_workers = []

    replacement_number = 0

    recovery_respawn_times = deque()

    for worker_number in range(RECOVERY_WORKER_COUNT):

        worker = Thread(
            target=failed_telemetry_worker,
            daemon=True,
            name=(
                f"failed-telemetry-recovery-{worker_number+1}"
            )
        )

        worker.start()
        recovery_workers.append(worker)
        print(f"Started {worker.name}")

    while True:

        for worker in recovery_workers.copy():
            now = time.monotonic()
            with recovery_heartbeat_lock:
                last_heartbeat = (recovery_worker_heartbeats.get(worker.name))

                heartbeat_timed_out = (
                    last_heartbeat is None or now - last_heartbeat > RECOVERY_WORKER_TIMEOUT
                )

                worker_dead = not worker.is_alive()

                if worker_dead or heartbeat_timed_out:
                    if worker_dead:
                        reason = "died"
                    else:
                        reason = "heartbeat timed out"

                    print(f"{worker.name} {reason}")

                while(
                    recovery_respawn_times and 
                    now - recovery_respawn_times[0] > RECOVERY_RESPAWN_WINDOW_SECONDS
                ):
                    recovery_respawn_times.popleft()

                if len(recovery_respawn_times) >= MAX_RECOVERY_RESPAWNS:

                    print(
                        f"{worker.name} "
                        f"Respawn limit reached. "
                        f"Not restarting worker."
                        )

                    recovery_workers.remove(worker)

                    continue

                recovery_workers.remove(worker)
                replacement_number += 1

                replacement = Thread(
                    target= failed_telemetry_worker,
                    daemon= True,
                    name= (f"failed-telemetry-recovery-replacement-{replacement_number}"
                )
            )
            replacement.start()

            recovery_workers.append(replacement)
            print(f"Start {replacement.name}")

            recovery_respawn_times.append(time.monotonic())
            print(f"Started {replacement.name}")

        time.sleep(RECOVERY_SUPERVISOR_CHECK_INTERVAL)

# =========================
# Worker supervisor
# =========================

def telemetry_worker_supervisor():

    workers = []

    replacement_number = 0

    respawn_times = deque()

    # -------------------------
    # Start initial workers
    # -------------------------

    for worker_number in range(WORKER_COUNT):

        worker = Thread(
            target=telemetry_worker,
            daemon=True,
            name=f"telemetry-worker-{worker_number + 1}"
        )

        worker.start()

        workers.append(worker)

        print(
            f"Started {worker.name}"
        )

    # -------------------------
    # Supervisor loop
    # -------------------------

    while True:

        for worker in workers.copy():

            now = time.monotonic()

            # =========================
            # Check worker heartbeat
            # =========================

            with heartbeat_lock:
                last_heartbeat = worker_heartbeats.get(worker.name)

            heartbeat_timed_out = (
                last_heartbeat is None
                or now - last_heartbeat > WORKER_TIMEOUT
            )

            worker_dead = not worker.is_alive()

            # Worker is unhealthy if:
            # 1. Thread died
            # OR
            # 2. Heartbeat stopped

            if worker_dead or heartbeat_timed_out:

                if worker_dead:

                    reason = "died"
                elif heartbeat_timed_out:
                    reason = "heartbeat timed out"

                else:
                    continue

                print(
                    f"{worker.name} {reason}."
                )

                # =========================
                # Remove old respawn attempts
                # =========================

                while (
                    respawn_times
                    and now - respawn_times[0]
                    > RESPAWN_WINDOW_SECONDS
                ):
                    respawn_times.popleft()

                # =========================
                # Respawn limit
                # =========================

                if len(respawn_times) >= MAX_RESPAWNS:

                    print(
                        f"{worker.name} "
                        f"Respawn limit reached. "
                        f"Not restarting worker."
                    )

                    workers.remove(worker)

                    continue

                # =========================
                # Remove dead worker
                # =========================

                workers.remove(worker)

                # =========================
                # Backoff
                # =========================

                print(
                    f"Restarting {worker.name} "
                    f"in {RESTART_BACKOFF_SECONDS} seconds."
                )

                time.sleep(RESTART_BACKOFF_SECONDS)

                # =========================
                # Create replacement
                # =========================

                replacement_number += 1

                replacement = Thread(
                    target=telemetry_worker,
                    daemon=True,
                    name=(
                        f"telemetry-worker-replacement-"
                        f"{replacement_number}"
                    )
                )

                replacement.start()

                workers.append(replacement)

                respawn_times.append(
                    time.monotonic()
                )

                print(
                    f"Started {replacement.name}"
                )

        time.sleep(SUPERVISOR_CHECK_INTERVAL)

def start_telemetry_workers():

    supervisor = Thread(
        target=telemetry_worker_supervisor,
        daemon=True,
        name="telemetry-worker-supervisor"
    )

    supervisor.start()

    print("Started telemetry worker supervisor")

    recovery_supervisor = Thread(
        target=recovery_worker_supervisor,
        daemon=True,
        name="failed-telemetry-supervisor"
    )

    recovery_supervisor.start()

    print(
        "Started failed telemetry supervisor"
    )
