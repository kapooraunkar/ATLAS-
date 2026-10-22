import time
from queue import PriorityQueue,Full
from enum import Enum
from dataclasses import dataclass

class QueueState(Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    CRITICAL = "critical"

@dataclass
class FailedTelemetry:
    telemetry: object
    retry_count: int = 0
    next_retry_time: float = 0.0

MAX_QUEUE_SIZE = 10000
FAILED_QUEUE_SIZE = 1000

DLQ_SIZE = 5000
MAX_RECOVERY_RETRIES = 10

failed_telemetry_queue =PriorityQueue(maxsize=FAILED_QUEUE_SIZE)
dead_letter_queue = PriorityQueue(maxsize=DLQ_SIZE)

telemetry_queue = PriorityQueue(maxsize=MAX_QUEUE_SIZE)

queue_state = QueueState.NORMAL

ingested_count = 0
processed_count = 0

last_rate_check = time.monotonic()
last_ingested_count = 0
last_processed_count = 0

sequence_number = 0
failed_sequence_number = 0


def get_priority(status_code: int) -> int:

    if status_code >= 500:  #critical
        return 1

    if status_code >= 400:  #important 
        return 2
 
    return 3     #normal


def add_to_queue(telemetry):

    global sequence_number

    priority = get_priority(telemetry.status_code)
    state = get_queue_state()

    # NORMAL:
    # Accept everything.
    if state == QueueState.NORMAL:
        pass

    # PRESSURE:
    # Drop normal telemetry.
    # Keep 4xx and 5xx.
    elif state == QueueState.PRESSURE:

        if priority >= 3:
            print(
                f"Queue pressure. "
                f"Dropping normal telemetry "
                f"{telemetry.request_id} "
                f"(status {telemetry.status_code})"
            )

            return False

    # CRITICAL:
    # Only keep 5xx telemetry.
    elif state == QueueState.CRITICAL:

        if priority >= 2:
            print(
                f"Critical queue pressure. "
                f"Dropping non-critical telemetry "
                f"{telemetry.request_id} "
                f"(status {telemetry.status_code})"
            )

            return False

    # Telemetry has passed the pressure policy.
    # Now attempt to put it into the priority queue.

    sequence_number += 1

    item = (priority, sequence_number, telemetry)

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

        while not telemetry_queue.empty():
            items.append(telemetry_queue.get_nowait())

        # Find the least important telemetry.
        worst_item = max(items, key=lambda x: x[0])

        # Only evict if the incoming telemetry
        # is more important than the worst item.
        if priority < worst_item[0]:

            print(
                f"Queue full. "
                f"Evicting telemetry "
                f"{worst_item[2].request_id} "
                f"(status {worst_item[2].status_code}) "
                f"for incoming telemetry "
                f"{telemetry.request_id} "
                f"(status {telemetry.status_code})"
            )

            items.remove(worst_item)

            for existing_item in items:
                telemetry_queue.put_nowait(existing_item)

            telemetry_queue.put_nowait(item)

            return True

        else:

            # Incoming telemetry isn't more important
            # than anything already in the queue.

            for existing_item in items:
                telemetry_queue.put_nowait(existing_item)

            return False

def get_queue_size() -> int:
    return telemetry_queue.qsize()


def get_queue_capacity() -> int:
    return MAX_QUEUE_SIZE


def get_queue_usage_percent() -> float:
    return (telemetry_queue.qsize() / MAX_QUEUE_SIZE) * 100


def record_ingested():
    global ingested_count
    ingested_count += 1


def record_processed():
    global processed_count
    processed_count += 1


def record_dropped_telemetry():
    pass


def get_rates():

    global last_rate_check
    global last_ingested_count
    global last_processed_count

    now = time.monotonic()
    elapsed = now - last_rate_check

    if elapsed == 0:
        return 0.0, 0.0

    ingestion_rate = (
        ingested_count - last_ingested_count
    ) / elapsed

    processing_rate = (
        processed_count - last_processed_count
    ) / elapsed

    last_rate_check = now
    last_ingested_count = ingested_count
    last_processed_count = processed_count

    return ingestion_rate, processing_rate

def get_queue_state() -> QueueState:

    global queue_state

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


def add_failed_telemetry(telemetry):

    global failed_sequence_number

    failed_item = FailedTelemetry(
        telemetry=telemetry,
        retry_count=0,
        next_retry_time=time.monotonic()+10
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

    try:

        dead_letter_queue.put_nowait(
            (
                time.monotonic(),
                failed_sequence_number,
                failed_item
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