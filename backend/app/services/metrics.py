from app.services.telemetry_queue import (
    get_metrics as get_queue_metrics,
    get_queue_size,
    get_queue_capacity,
    get_queue_usage_percent,
    dead_letter_queue,
)
from app.services.telemetry_worker import (
    worker_heartbeats,
    recovery_worker_heartbeats,
    heartbeat_lock,
    recovery_heartbeat_lock,
)

from app.config import settings

from app.services.persistence_health import persistence_health

def get_internal_metrics():

    with heartbeat_lock:
        telemetry_workers_alive = len(worker_heartbeats)

    with recovery_heartbeat_lock:
        recovery_workers_alive = len(recovery_worker_heartbeats)

    queue = get_queue_metrics()
    persistence = persistence_health.get_metrics()

    return {
        "telemetry" : {
        "ingested":  queue.ingested,
        "processed": queue.processed,
        "dropped": queue.dropped,
        },

        "queue": {
            "depth": get_queue_size(),
            "capacity": get_queue_capacity(),
            "usage_percent": get_queue_usage_percent(),
            "oldest_telemetry_age": queue.oldest_telemetry_age,
            "backpressure_level": queue.backpressure_level.value,
            "ingestion_rate": queue.ingested_rate,
            "processing_rate": queue.processing_rate,
        },

        "persistence": {
            "attempts": persistence["attempts"],
            "failure_rate": persistence["failure_rate"],
            "average_latency": persistence["average_latency"],
            "state": persistence_health.get_state().value,

        },

        "dlq": {
            "depth": dead_letter_queue.qsize(),
        },

        "workers": {
            "telemetry": {
                "expected": settings.worker_count,
                "alive": telemetry_workers_alive,
            },
            "recovery": {
                "expected": settings.recovery_worker_count,
                "alive": recovery_workers_alive,
            },
        },
    }
