Atlas doesn't compete with observability platforms. It competes with uncertainty.

# Day 1

## Decision 1

Atlas is an AI Infrastructure Intelligence Platform.

Reason:
Observability platforms already collect telemetry.
Atlas focuses on understanding, reasoning, predicting, and assisting.

---

## Decision 2

Atlas integrates with existing infrastructure instead of replacing it.

Reason:
Organizations won't replace their databases, cloud providers, or applications for an observability platform.

---

## Decision 3

Atlas augments engineers instead of replacing them.

Reason:
Production decisions require human judgment.

## Day 2

### Decision 4

Atlas will initially be built as a Modular Monolith.

**Reason**

A modular monolith provides faster development, easier debugging, and lower operational complexity during the early stages of the project.

Services will only be extracted when there is a clear technical or business justification.

---

### Decision 5

Microservices will be introduced only to solve real engineering problems.

**Reason**

Traffic alone is not a sufficient reason to adopt microservices. Service boundaries should be driven by business domains, team ownership, deployment independence, scalability requirements, and fault isolation.

##Decision

Business responses and telemetry are separate concerns.

## Reason

End users should receive clear, safe, and actionable responses, while Atlas records detailed telemetry for engineers. Internal implementation details must never be exposed through user-facing error messages.

## Day 3

### Decision 6

Atlas will use `time.perf_counter()` to measure request duration.

**Reason**

Atlas measures elapsed execution time rather than wall-clock time. `time.perf_counter()` provides a high-resolution monotonic timer that is unaffected by system clock adjustments, making it more reliable for request latency measurements.

### Decision 7

Atlas records every request regardless of outcome.

## Reason:

Successful requests, client errors (4xx), server errors (5xx), and unexpected exceptions all provide valuable operational insight. Atlas observes reality rather than filtering it.

### Decision 8

Telemetry will be represented as a single object throughout the system.

**Reason**

Telemetry is a single domain entity. Passing one object instead of many parameters makes APIs cleaner, easier to extend, and reduces coupling between components. New telemetry fields can be added without changing function signatures.

Variables and fields should encode their units whenever ambiguity exists.

Good

duration_ms
cpu_percent
memory_percent
disk_usage_bytes

Bad

duration
cpu
memory
disk

ATLAS
│
├── Resilient infrastructure
│   ├── Queue
│   ├── Workers
│   ├── Persistence
│   ├── Recovery
│   └── Failure isolation
│
├── Self-observability
│   ├── Atlas monitors Atlas
│   ├── Health + telemetry
│   ├── Capacity/pressure
│   └── Internal failure detection
│
├── Infrastructure observability
│   ├── Metrics
│   ├── Logs
│   ├── Traces
│   └── Infrastructure signals
│
├── Intelligence
│   ├── Anomaly detection
│   ├── Spike prediction
│   ├── Forecasting
│   ├── Correlation
│   └── Root-cause analysis
│
├── AI layer
│   ├── LLM
│   ├── RAG
│   ├── Vector DB
│   └── Historical/contextual reasoning
│
└── Remediation
    ├── Recommendations
    └── Eventually automated actions