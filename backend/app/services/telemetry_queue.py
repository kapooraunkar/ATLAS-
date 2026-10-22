import time
from queue import PriorityQueue,Full
from enum import Enum
from dataclasses import dataclass
from threading import RLock
from dataclasses import dataclass


class QueueState(Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    CRITICAL = "critical"

class AdmissionDecision(Enum):
    ACCEPT = "accept"
    DROP = "drop"

class AdmissionMode(Enum):
    ACCEPT_ALL = "accept_all"
    SHED_NORMAL = "shed_normal"
    SHED_NON_CRITICAL = "shed_non critical"


class BackpressureLevel(Enum):
    NONE = "none"
    ELEVATED = "elevated"
    HIGH = "high"
    SEVERE = "severe"


class LoadSheddingPolicy:
    def decide(self,queue_state: QueueState, priority: int) -> AdmissionDecision:

        if queue_state == QueueState.NORMAL:
            return AdmissionDecision.ACCEPT

        if queue_state == QueueState.PRESSURE:
            if priority >= 3:
                return AdmissionDecision.DROP
            return AdmissionDecision.ACCEPT

        if queue_state == QueueState.CRITICAL:
            if priority >= 2:
                return AdmissionDecision.DROP
            return AdmissionDecision.ACCEPT

load_shedding_policy = LoadSheddingPolicy()

@dataclass
class QueueItem:
    priority: int
    sequence_number: int
    enqueued_at: float
    telemetry: object

@dataclass
class FailedTelemetry:
    telemetry: object
    retry_count: int = 0
    next_retry_time: float = 0.0
    original_enqueued_at: float = 0.0
    last_error: str = ""

@dataclass
class DeadLetterItem:
    telemetry: object
    original_enqueued_at: float
    dead_lettered_at: float
    retry_count: int
    last_error: str
    source: str


@dataclass
class QueueMetrics:
    ingested: int
    processed: int
    dropped: int
    ingested_rate: int
    processing_rate: float
    oldest_telemetry_age: float
    backpressure_level: BackpressureLevel


MAX_QUEUE_SIZE = 10
FAILED_QUEUE_SIZE = 1000

DLQ_SIZE = 5000
MAX_RECOVERY_RETRIES = 3

FAILED_RETRY_INTERVAL = 10

failed_telemetry_queue =PriorityQueue(maxsize=FAILED_QUEUE_SIZE)
dead_letter_queue = PriorityQueue(maxsize=DLQ_SIZE)

telemetry_queue = PriorityQueue(maxsize=MAX_QUEUE_SIZE)

queue_state = QueueState.NORMAL
queue_state_lock = RLock()
metrics_lock = RLock()

admission_mode = AdmissionMode.ACCEPT_ALL
admission_mode_lock = RLock()


dropped_count = 0
ingested_count = 0
processed_count = 0

last_rate_check = time.monotonic()
last_ingested_count = 0
last_processed_count = 0

last_ingestion_rate = 0.0
last_processing_rate = 0.0

sequence_number = 0
failed_sequence_number = 0

def set_admission_mode(mode: AdmissionMode):
    global admission_mode

    with admission_mode_lock:
        previous_mode = admission_mode
        admission_mode = mode

    if previous_mode != mode:
        print(
            f"Atlas admission mode changed: "
            f"{previous_mode.value.upper()} -> {mode.value.upper()}"
        )

def get_admission_mode() -> AdmissionMode:
    with admission_mode_lock:
        return admission_mode

def get_backpressure_level() -> BackpressureLevel:

    queue_usage = get_queue_usage_percent()

    with metrics_lock:
        ingestion_rate = last_ingestion_rate
        processing_rate = last_processing_rate

    oldest_age = get_oldest_telemetry_age()

    if queue_usage >= 95 or oldest_age >= 10:
        return BackpressureLevel.SEVERE

    if queue_usage >= 80 or oldest_age >= 5:
        return BackpressureLevel.HIGH

    if ingestion_rate > processing_rate * 1.2:
        return BackpressureLevel.ELEVATED

    return BackpressureLevel.NONE


def get_priority(status_code: int) -> int:

    if status_code >= 500:  #critical
        return 1

    if status_code >= 400:  #important
        return 2

    return 3     #normal


def add_to_queue(telemetry):

    queue_lock = RLock()

    global sequence_number

    priority = get_priority(telemetry.status_code)
    state = get_queue_state()

    decision = load_shedding_policy.decide(
        queue_state=state,
        priority=priority
    )

    if decision == AdmissionDecision.DROP:
            print(
                f"Load Shedding. "
                f"Dropping telemetry "
                f"{telemetry.request_id} "
                f"(status {telemetry.status_code}, "
                f"queue state: {state.value})"
            )

            record_dropped_telemetry()

            return False

    # Telemetry has passed the pressure policy.
    # Now attempt to put it into the priority queue.
    with queue_lock:
        sequence_number += 1

        item = (
            priority,
            sequence_number,
            QueueItem(
            priority=priority,
            sequence_number=sequence_number,
            enqueued_at=time.monotonic(),
            telemetry=telemetry

            )
        )

        try:

            telemetry_queue.put_nowait(item)

            return True

        except Full:

            # Queue is completely full.

            # Normal telemetry should already have been
            # rejected by the pressure policy.
            if priority >= 3:
                return False

            items = []

            for _ in range(MAX_QUEUE_SIZE):
                items.append(telemetry_queue.get_nowait())
            # Find the least important telemetry.
            worst_item = max(items, key=lambda x: x.priority)

            # Only evict if the incoming telemetry
            # is more important than the worst item.
            if priority < worst_item[0]:
                items.remove(worst_item)

                record_dropped_telemetry()

                for existing_item in items:
                    telemetry_queue.put_nowait(existing_item)

                telemetry_queue.put_nowait(item)

                print(
                    f"Queue full. "
                    f"Evicting telemetry "
                    f"{worst_item[2].telemetry.request_id} "
                    f"(status {worst_item[2].telemetry.status_code}) "
                    f"for incoming telemetry "
                    f"{telemetry.request_id} "
                    f"(status {telemetry.status_code})"
                )

                return True

            else:

                # Incoming telemetry isn't more important
                # than anything already in the queue.

                for existing_item in items:
                    telemetry_queue.put_nowait(existing_item)

                return False

def get_oldest_telemetry_age() -> float:
    if telemetry_queue.empty():
        return 0.0

    oldest_item = min(
        list(telemetry_queue.queue),
        key=lambda x: x[2].enqueued_at
    )

    return time.monotonic() - oldest_item[2].enqueued_at

def get_queue_size() -> int:
    return telemetry_queue.qsize()


def get_queue_capacity() -> int:
    return MAX_QUEUE_SIZE


def get_queue_usage_percent() -> float:
    return (telemetry_queue.qsize() / MAX_QUEUE_SIZE) * 100


def record_ingested():
    global ingested_count

    with metrics_lock:
        ingested_count += 1

def get_metrics():

    backpressure_level = get_backpressure_level()

    with metrics_lock:
        return QueueMetrics(
            ingested=ingested_count,
            processed=processed_count,
            dropped=dropped_count,
            ingested_rate=last_ingestion_rate,
            processing_rate=last_processing_rate,
            oldest_telemetry_age=get_oldest_telemetry_age(),
            backpressure_level=backpressure_level,
        )


def record_processed():
    global processed_count

    with metrics_lock:
        processed_count += 1


def record_dropped_telemetry():

    global dropped_count

    with metrics_lock:
        dropped_count += 1

def get_rates():

    global last_rate_check
    global last_ingested_count
    global last_processed_count
    global last_ingestion_rate
    global last_processing_rate

    now = time.monotonic()

    with metrics_lock:
        elapsed = now - last_rate_check

        if elapsed == 0:
            return 0.0, 0.0

        current_ingested = ingested_count
        current_processed = processed_count

        ingestion_rate = (
            current_ingested - last_ingested_count
        ) / elapsed

        processing_rate = (
            current_processed - last_processed_count
        ) / elapsed

        last_rate_check = now
        last_ingested_count = ingested_count
        last_processed_count = processed_count

    return ingestion_rate, processing_rate

def get_queue_state() -> QueueState:

    global queue_state

    with queue_state_lock:
        usage = get_queue_usage_percent()
        previous_state = queue_state

        if queue_state == QueueState.NORMAL:

            if usage >= 80:
                queue_state = QueueState.PRESSURE

        elif queue_state == QueueState.PRESSURE:

            if usage >= 95:
                queue_state = QueueState.CRITICAL

            elif usage <= 70:
                queue_state = QueueState.NORMAL

        elif queue_state == QueueState.CRITICAL:

            if usage <= 90:
                queue_state = QueueState.PRESSURE

        if queue_state != previous_state:

            print(
                f"Atlas queue state changed: "
                f"{previous_state.value.upper()} → "
                f"{queue_state.value.upper()} "
                f"(queue usage: {usage:.2f}%)"
            )

        return queue_state


def add_failed_telemetry(telemetry, original_enqueued_at=0.0):

    global failed_sequence_number

    failed_item = FailedTelemetry(
        telemetry=telemetry,
        retry_count=0,
        next_retry_time=time.monotonic()+ FAILED_RETRY_INTERVAL,
        original_enqueued_at=original_enqueued_at
    )

    failed_sequence_number += 1

    try:

        failed_telemetry_queue.put_nowait(
            (
            failed_item.next_retry_time,
            failed_sequence_number,
            failed_item

        )
    )

        print(
            f"Telemetry moved to failed queue: {telemetry.request_id}"
        )

        return True

    except Full:

        print(f"FAILED QUEUE FULL. "
            f"Unable to preserve telemetry: "
            f"{telemetry.request_id}"
        )

        return False


def requeue_failed_telemetry(failed_item):

    global failed_sequence_number

    failed_sequence_number += 1
    failed_telemetry_queue.put_nowait(
        (
            failed_item.next_retry_time,
            failed_sequence_number,
            failed_item
        )
    )

def move_to_dead_letter(failed_item):
    global failed_sequence_number

    failed_sequence_number += 1

    dead_letter_item = DeadLetterItem(
        telemetry=failed_item.telemetry,
        original_enqueued_at=failed_item.original_enqueued_at,
        dead_lettered_at=time.monotonic(),
        retry_count=failed_item.retry_count,
        last_error=failed_item.last_error,
        source="recovery"
    )

    try:

        dead_letter_queue.put_nowait(
            (
                time.monotonic(),
                failed_sequence_number,
                dead_letter_item
            )
        )

        print(
            f"Telemetry moved to DLQ: {failed_item.telemetry.request_id}"
            f"(retries: {failed_item.retry_count})"
        )

        return True

    except Full:

        print(
            f"DLQ FULL. "
            f"Unable to preserve telemetry: "
            f"{failed_item.telemetry.request_id}"
        )

        return False

def get_dead_letter_items():
    return list(dead_letter_queue.queue)


def inspect_dead_letter_queue():
    items = get_dead_letter_items()

    if not items:
        print("DLQ is empty,")
        return

    print("\n========= DEAD LETTER QUEUE ==========")

    for timestamp, sequence_number, item in items:
        print(
            f"Sequence: {sequence_number} | "
            f"Request: {item.telemetry.request_id}"
        )

        print(
            f"Original ingestion: {item.original_enqueued_at:.2f}"
        )

        print(
            f"Dead-lettered: {item.dead_lettered_at:.2f}"
        )

        print(
            f"Retries: {item.retry_count}"
        )

        print(
            f"Last error: {item.last_error}"
        )

        print(
            f"Source: {item.source}"
        )
