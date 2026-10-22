import time
from threading import Thread, Lock, current_thread, Event
from collections import deque
from queue import Empty
from enum import Enum

from app.config import settings
from app.services.logger import logger,request_id_context

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
    MAX_RECOVERY_RETRIES,
)

from app.services.system_health import system_health_bus, HealthEvent
from app.services.circuit_breaker import CircuitBreaker

from app.services.telemetry_service import (
    process_telemetry,
    check_persistence_health,
)

from app.services.persistence_health import (
    persistence_health,
    PersistenceState,
)


class RecoveryMode(Enum):
    NORMAL = "normal"
    LIMITED = "limited"
    PAUSED = "paused"


# =========================
# Worker heartbeat state
# =========================

worker_heartbeats = {}
heartbeat_lock = Lock()

worker_shutdown_event = Event()

telemetry_supervisor = None
recovery_supervisor = None

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

        stop_event.wait(settings.heartbeat_interval)

    # Remove heartbeat when worker is stopping
    with heartbeat_lock:
        worker_heartbeats.pop(worker_name, None)


# =========================
# Recovery Worker Heartbeat
# =========================

def recovery_worker_heartbeat(worker_name, stop_event):

    while not stop_event.is_set():

        with recovery_heartbeat_lock:
            recovery_worker_heartbeats[worker_name] = time.monotonic()

        stop_event.wait(settings.recovery_heartbeat_interval)

    with recovery_heartbeat_lock:
        recovery_worker_heartbeats.pop(worker_name, None)


def should_probe_persistence():

    global last_persistence_probe

    now = time.monotonic()

    with persistence_probe_lock:

        if now - last_persistence_probe < settings.persistence_probe_interval:
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

def telemetry_worker(stop_event):

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
        name=f"{worker_name}-heartbeat",
    )

    heartbeat.start()

    # Worker local circuit breaker
    circuit = CircuitBreaker()

    # Subscribe this worker to system health announcements
    health_handler = create_health_handler(circuit)
    system_health_bus.subscribe(health_handler)

    try:

        while not stop_event.is_set():

            try:

                _, _, queue_item = telemetry_queue.get(timeout=1)

            except Empty:
                continue

            telemetry = queue_item.telemetry
            original_enqueued_at = queue_item.enqueued_at
            token = request_id_context.set(str(telemetry.request_id))

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

                if not circuit.allow_request():

                    logger.warning(
                        f"Circuit OPEN for worker {worker_name}. "
                        f"Deferring telemetry {telemetry.request_id}"
                    )

                    add_failed_telemetry(
                        telemetry,
                        original_enqueued_at
                    )

                    continue

                for attempt in range(
                    1,
                    settings.max_retries + 1
                ):

                    start_time = time.monotonic()

                    try:

                        process_telemetry(telemetry)

                        latency = time.monotonic() - start_time

                        persistence_health.record_persistence_attempt(
                            success=True,
                            latency=latency,
                        )

                        circuit.record_success()

                        record_processed()

                        get_queue_state()

                        logger.info(
                            f"Telemetry persisted successfully "
                            f"attempt {attempt}"
                        )

                        persisted = True
                        break

                    except Exception as e:

                        latency = time.monotonic() - start_time

                        persistence_health.record_persistence_attempt(
                            success=False,
                            latency=latency,
                        )

                        circuit.record_failure()

                        logger.error(
                            f"Telemetry persistence failed "
                            f"(attempt {attempt}/"
                            f"{settings.max_retries}): {e}"
                        )

                        if attempt < settings.max_retries:

                            stop_event.wait(
                                settings.retry_delay_seconds
                            )

                        else:

                            logger.error(
                                f"Telemetry persistence failed "
                                f"for request {telemetry.request_id}"
                            )

                # This is after all retry attempts
                if not persisted:

                    logger.error(
                        f"Telemetry permanently failed "
                        f"for request {telemetry.request_id}"
                    )

                    add_failed_telemetry(
                        telemetry,
                        original_enqueued_at
                    )

            finally:
                request_id_context.reset(token)
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

def failed_telemetry_worker(stop_event):

    worker_name = current_thread().name

    circuit = CircuitBreaker()

    stop_heartbeat = Event()

    health_handler = create_health_handler(circuit)
    system_health_bus.subscribe(health_handler)

    with recovery_heartbeat_lock:

        recovery_worker_heartbeats[worker_name] = time.monotonic()

        heartbeat = Thread(
            target=recovery_worker_heartbeat,
            args=(
                worker_name,
                stop_heartbeat,
            ),
            daemon=True,
            name=f"{worker_name}-heartbeat",
        )

        heartbeat.start()

    try:

        while not stop_event.is_set():

            try:

                next_retry_time, _, failed_item = (
                    failed_telemetry_queue.get(timeout=1)
                )

                token = request_id_context.set(str(failed_item.telemetry.request_id))

            except Empty:
                continue

            now = time.monotonic()

            if now < next_retry_time:

                stop_event.wait(next_retry_time - now)

                now = time.monotonic()

            mode = get_recovery_mode()

            print(
                f"Recovery worker {worker_name}: "
                f"mode={mode.value}, "
                f"retry_count={failed_item.retry_count}, "
                f"circuit={circuit.get_state().value}"
            )

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
                        latency=latency,
                    )

                failed_item.next_retry_time = (
                    time.monotonic()
                    + settings.failed_retry_interval
                )

                requeue_failed_telemetry(
                    failed_item
                )

                continue

            # =========================
            # LIMITED
            # =========================

            if mode == RecoveryMode.LIMITED:

                logger.warning(
                    f"Persistence degraded. "
                    f"Delaying recovery for "
                    f"{settings.limited_recovery_delay}s"
                )

                stop_event.wait(
                    settings.limited_recovery_delay
                )

            # =========================
            # NORMAL + LIMITED
            # =========================

            if not circuit.allow_request():

                logger.warning(
                    f"Recovery circuit OPEN for "
                    f"{worker_name}. "
                    f"Deferring telemetry "
                    f"{failed_item.telemetry.request_id}"
                )

                failed_item.next_retry_time = (
                    time.monotonic()
                    + settings.failed_retry_interval
                )

                requeue_failed_telemetry(
                    failed_item
                )

                continue

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
                    latency=latency,
                )

                circuit.record_success()

                logger.info(
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
                    latency=latency,
                )

                circuit.record_failure()

                failed_item.last_error = str(e)

                failed_item.retry_count += 1

                if failed_item.retry_count >= MAX_RECOVERY_RETRIES:

                    logger.error(
                        f"Telemetry moved to DLQ: "
                        f"{failed_item.telemetry.request_id} "
                        f"after {failed_item.retry_count} retries"
                    )

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

                logger.warning(
                    f"Recovery failed for "
                    f"{failed_item.telemetry.request_id}. "
                    f"Retry #{failed_item.retry_count} "
                    f"in {backoff}s: {e}"
                )

                requeue_failed_telemetry(
                    failed_item
                )

            finally:
                request_id_context.reset(token)
                failed_telemetry_queue.task_done()

    finally:

        stop_heartbeat.set()

        with recovery_heartbeat_lock:
            recovery_worker_heartbeats.pop(
                worker_name,
                None
            )


# =========================
# HANDLER FACTORY
# =========================

def create_health_handler(circuit):

    def handle_health_event(event):

        logger.info(
            f"Worker received health event: {event.value}"
        )

        if event == HealthEvent.PERSISTENCE_FAILURE:

            logger.error(
                "Worker received persistence failure announcement"
            )

            circuit.respond_to_system_failure()

        elif event == HealthEvent.PERSISTENCE_RECOVERED:

            logger.info(
                "Worker received persistence recovery announcement"
            )

            circuit.respond_to_system_recovery()

    return handle_health_event


# =========================
# Recovery Worker supervisor
# =========================

def recovery_worker_supervisor(stop_event):

    recovery_workers = []

    replacement_number = 0

    recovery_respawn_times = deque()

    for worker_number in range(
        settings.recovery_worker_count
    ):

        worker = Thread(
            target=failed_telemetry_worker,
            args=(stop_event,),
            daemon=True,
            name=(
                f"failed-telemetry-recovery-"
                f"{worker_number + 1}"
            ),
        )

        worker.start()

        recovery_workers.append(worker)

        logger.info(
            f"Started {worker.name}"
        )

    while not stop_event.is_set():

        for worker in recovery_workers.copy():

            now = time.monotonic()

            with recovery_heartbeat_lock:

                last_heartbeat = (
                    recovery_worker_heartbeats.get(
                        worker.name
                    )
                )

                heartbeat_timed_out = (
                    last_heartbeat is None
                    or now - last_heartbeat
                    > settings.recovery_worker_timeout
                )

                worker_dead = not worker.is_alive()

            if worker_dead or heartbeat_timed_out:

                if worker_dead:
                    reason = "died"
                else:
                    reason = "heartbeat timed out"

                logger.error(
                    f"{worker.name} {reason}"
                )

                while (
                    recovery_respawn_times
                    and now - recovery_respawn_times[0]
                    > settings.recovery_respawn_window_seconds
                ):

                    recovery_respawn_times.popleft()

                if (
                    len(recovery_respawn_times)
                    >= settings.max_recovery_respawns
                ):

                    logger.error(
                        f"{worker.name} "
                        f"Respawn limit reached. "
                        f"Not restarting worker."
                    )

                    recovery_workers.remove(
                        worker
                    )

                    continue

                recovery_workers.remove(
                    worker
                )

                replacement_number += 1

                replacement = Thread(
                    target=failed_telemetry_worker,
                    args=(stop_event,),
                    daemon=True,
                    name=(
                        f"failed-telemetry-recovery-"
                        f"replacement-"
                        f"{replacement_number}"
                    ),
                )

                replacement.start()

                recovery_workers.append(
                    replacement
                )

                recovery_respawn_times.append(
                    time.monotonic()
                )

                logger.warning(
                    f"Started {replacement.name}"
                )

        stop_event.wait(
            settings.recovery_supervisor_check_interval
        )


# =========================
# Worker supervisor
# =========================

def telemetry_worker_supervisor(stop_event):

    workers = []

    replacement_number = 0

    respawn_times = deque()

    # -------------------------
    # Start initial workers
    # -------------------------

    for worker_number in range(
        settings.worker_count
    ):

        worker = Thread(
            target=telemetry_worker,
            args=(stop_event,),
            daemon=True,
            name=f"telemetry-worker-{worker_number + 1}",
        )

        worker.start()

        workers.append(worker)

        logger.info(
            f"Started {worker.name}"
        )

    # -------------------------
    # Supervisor loop
    # -------------------------

    while not stop_event.is_set():

        for worker in workers.copy():

            now = time.monotonic()

            # =========================
            # Check worker heartbeat
            # =========================

            with heartbeat_lock:

                last_heartbeat = (
                    worker_heartbeats.get(
                        worker.name
                    )
                )

                heartbeat_timed_out = (
                    last_heartbeat is None
                    or now - last_heartbeat
                    > settings.worker_timeout
                )

                worker_dead = not worker.is_alive()

            if worker_dead or heartbeat_timed_out:

                if worker_dead:

                    reason = "died"

                else:

                    reason = "heartbeat timed out"

                logger.error(
                    f"{worker.name} {reason}"
                )

                # =========================
                # Remove old respawn attempts
                # =========================

                while (
                    respawn_times
                    and now - respawn_times[0]
                    > settings.respawn_window_seconds
                ):

                    respawn_times.popleft()

                # =========================
                # Respawn limit
                # =========================

                if (
                    len(respawn_times)
                    >= settings.max_respawns
                ):

                    logger.error(
                        f"{worker.name} "
                        f"Respawn limit reached. "
                        f"Not restarting worker."
                    )

                    workers.remove(
                        worker
                    )

                    continue

                # =========================
                # Remove dead worker
                # =========================

                workers.remove(
                    worker
                )

                # =========================
                # Backoff
                # =========================

                logger.warning(
                    f"Restarting {worker.name} "
                    f"in {settings.restart_backoff_seconds} seconds."
                )

                time.sleep(
                    settings.restart_backoff_seconds
                )

                # =========================
                # Create replacement
                # =========================

                replacement_number += 1

                replacement = Thread(
                    target=telemetry_worker,
                    args=(stop_event,),
                    daemon=True,
                    name=(
                        f"telemetry-worker-replacement-"
                        f"{replacement_number}"
                    ),
                )

                replacement.start()

                workers.append(
                    replacement
                )

                respawn_times.append(
                    time.monotonic()
                )

                logger.info(
                    f"Started {replacement.name}"
                )

        stop_event.wait(
            settings.supervisor_check_interval
        )


def start_telemetry_workers():

    global telemetry_supervisor, recovery_supervisor

    worker_shutdown_event.clear()

    telemetry_supervisor = Thread(
        target=telemetry_worker_supervisor,
        args=(worker_shutdown_event,),
        daemon=True,
        name="telemetry-worker-supervisor",
    )

    telemetry_supervisor.start()

    logger.info(
        "Started telemetry worker supervisor"
    )

    recovery_supervisor = Thread(
        target=recovery_worker_supervisor,
        args=(worker_shutdown_event,),
        daemon=True,
        name="failed-telemetry-supervisor",
    )

    recovery_supervisor.start()

    logger.info(
        "Started failed telemetry supervisor"
    )


def stop_telemetry_workers():

    worker_shutdown_event.set()

    if telemetry_supervisor is not None:

        telemetry_supervisor.join(
            timeout=5
        )

    if recovery_supervisor is not None:

        recovery_supervisor.join(
            timeout=5
        )

    logger.info(
        "Telemetry workers stopped"
    )