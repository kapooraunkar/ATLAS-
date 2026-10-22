from prometheus_client import (
    Counter,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST
)

from app.services.metrics import get_internal_metrics

# Cumulative counters
telemetry_ingested = Counter(
    "atlas_telemetry_ingested_total",
    "Total telemetry items ingested by ATLAS",
)

telemetry_processed = Counter(
    "atlas_telemetry_processed_total",
    "Total telemetry items successfully persisted",
)

telemetry_dropped = Counter(
    "atlas_telemetry_dropped_total",
    "Total telemetry items dropped by ATLAS",
)

persistence_attempts = Counter(
    "atlas_persistence_attempts_total",
    "Total persistence attempts recorded by ATLAS",
)


# Current-state gauges
queue_depth = Gauge(
    "atlas_queue_depth",
    "Current number of telemetry items in the queue",
)

queue_capacity = Gauge(
    "atlas_queue_capacity",
    "Maximum telemetry queue capacity",
)

queue_usage_percent = Gauge(
    "atlas_queue_usage_percent",
    "Current telemetry queue utilization percentage",
)

ingestion_rate = Gauge(
    "atlas_ingestion_rate",
    "Current telemetry ingestion rate per second",
)

processing_rate = Gauge(
    "atlas_processing_rate",
    "Current telemetry processing rate per second",
)

persistence_failure_rate = Gauge(
    "atlas_persistence_failure_rate",
    "Fraction of persistence attempts that failed",
)

persistence_average_latency = Gauge(
    "atlas_persistence_average_latency_seconds",
    "Average persistence latency in seconds",
)

dlq_depth = Gauge(
    "atlas_dlq_depth",
    "Current number of items in the dead letter queue",
)

telemetry_workers_alive = Gauge(
    "atlas_telemetry_workers_alive",
    "Number of live telemetry workers",
)

recovery_workers_alive = Gauge(
    "atlas_recovery_workers_alive",
    "Number of live recovery workers",
)


def refresh_prometheus_metrics():
    metrics = get_internal_metrics()

    telemetry_ingested.inc(0)  # Initialize the metric if needed
    telemetry_processed.inc(0)
    telemetry_dropped.inc(0)
    persistence_attempts.inc(0)

    # Counters are cumulative. Set them to the totals exposed
    # by ATLAS instead of incrementing them on every scrape.
    telemetry_ingested._value.set(metrics["telemetry"]["ingested"])
    telemetry_processed._value.set(metrics["telemetry"]["processed"])
    telemetry_dropped._value.set(metrics["telemetry"]["dropped"])
    persistence_attempts._value.set(
        metrics["persistence"]["attempts"]
    )

    queue_depth.set(metrics["queue"]["depth"])
    queue_capacity.set(metrics["queue"]["capacity"])
    queue_usage_percent.set(metrics["queue"]["usage_percent"])

    ingestion_rate.set(metrics["queue"]["ingestion_rate"])
    processing_rate.set(metrics["queue"]["processing_rate"])

    persistence_failure_rate.set(
        metrics["persistence"]["failure_rate"]
    )
    persistence_average_latency.set(
        metrics["persistence"]["average_latency"]
    )

    dlq_depth.set(metrics["dlq"]["depth"])

    telemetry_workers_alive.set(
        metrics["workers"]["telemetry"]["alive"]
    )
    recovery_workers_alive.set(
        metrics["workers"]["recovery"]["alive"]
    )

    return generate_latest(), CONTENT_TYPE_LATEST
