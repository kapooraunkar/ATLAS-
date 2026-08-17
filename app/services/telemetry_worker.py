import time
from threading import Thread, Lock, current_thread, Event
from collections import deque

from app.services.telemetry_queue import (
    telemetry_queue,
    get_queue_usage_percent,
    record_processed,
    get_rates,
    get_queue_state,
    add_failed_telemetry,
    failed_telemetry_queue,

)

from app.services.telemetry_service import process_telemetry
from queue import Full,Empty

# =========================
# Worker configuration
# =========================

FAILED_RETRY_INTERVAL = 10

WORKER_COUNT = 2

HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 15

SUPERVISOR_CHECK_INTERVAL = 1

MAX_RESPAWNS = 3
RESPAWN_WINDOW_SECONDS = 10

RESTART_BACKOFF_SECONDS = 2

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1


# =========================
# Worker heartbeat state
# =========================

worker_heartbeats = {}
heartbeat_lock = Lock()


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

                    try:

                        process_telemetry(telemetry)
                        record_processed()

                        # Check whether queue pressure
                        # has recovered.
                        get_queue_state()

                        print(
                            f"Telemetry persisted successfully "
                            f"(attempt {attempt})"
                        )

                        persisted = True

                        break

                    except Exception as e:

                        print(
                            f"Telemetry persistence failed "
                            f"(attempt {attempt}/{MAX_RETRIES}): {e}"
                        )

                        if attempt < MAX_RETRIES:

                            time.sleep(RETRY_DELAY_SECONDS)

                        else:

                            print(
                                f"Telemetry permanently failed "
                                f"for request {telemetry.request_id}"
                            )

                            

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
    while True:

        try:
            telemetry = failed_telemetry_worker.get(timeout=FAILED_RETRY_INTERVAL)

        except Empty:
            continue

        try:

            print(
                f"Retrying failed telemetry "
                f"{telemetry.request_id}"
            )

            process_telemetry(telemetry)

            print(
                f"Failed telemetry recovered successfully "
                f"{telemetry.request_id}"
            )

        except Exception as e:

            print(
                f"Failed telemetry recovery failed "
                f"for {telemetry.request_id}: {e}"
            )

            #Put it back for a future recovery attempt
            try:
                failed_telemetry_worker.put_nowait(telemetry)

            except Full:
                print(
                    f"Failed telemetry queue full. "
                    f"Unable to requeue "
                    f"{telemetry.request_id}"
                )

        finally:

            failed_telemetry_queue.task_done()
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

    recovery_worker = Thread(
        target=failed_telemetry_worker,
        daemon=True,
        name="failed-telemetry-recovery"
    )

    recovery_worker.start()
    print("Started failed telemetry recovery worker")