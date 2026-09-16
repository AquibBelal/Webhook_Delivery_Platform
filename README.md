# High-Reliability Distributed Webhook Delivery Platform

A distributed, asynchronous webhook dispatching engine built with Python, FastAPI, Redis, and PostgreSQL. Engineered to guarantee at-least-once delivery semantics, non-blocking ingestion, HMAC-SHA256 payload integrity, exponential backoff with jitter, dead-letter queue (DLQ) isolation, and real-time observability.

---

## Architecture Overview

```mermaid
flowchart TD
    Client([Client / Upstream Service]) -->|POST /events + Idempotency-Key| API[FastAPI Ingestion Layer]

    subgraph Ingestion & Validation
        API -->|Atomic SETNX Lock| RedisLock[(Redis Distributed Lock)]
        API -->|Persist Event & State| DB[(PostgreSQL)]
        API -->|Enqueue Delivery ID| ReadyQueue[(Redis FIFO Queue)]
    end

    API -.->|202 Accepted <15ms| Client

    subgraph Dispatch Engine
        ReadyQueue -->|BLPOP| Worker[Async Worker Pool]
        Worker -->|Compute HMAC-SHA256| Signer[Payload Signer]
        Worker -->|HTTP POST with Timeout| Target[Target Customer Endpoint]
    end

    subgraph Failure Mitigation & Resilience
        Target -->|2xx Success| Complete[(Log Attempt & Update Status)]
        Target -->|5xx / Timeout / Network Error| Evaluator{Attempts < Max?}
        
        Evaluator -->|Yes| Jitter[Backoff + Jitter Calculation]
        Jitter -->|Schedule Execution Timestamp| DelayedZSet[(Redis Delayed ZSET)]
        DelayedZSet -->|Polled by Scheduler| ReadyQueue

        Evaluator -->|No| DLQ[(Redis Dead Letter Queue)]
        DLQ -->|Manual / Automated Trigger| Replay["/dlq/replay Endpoint"]
        Replay --> ReadyQueue
    end

    subgraph Observability
        Worker -->|Latency, Status Codes, Queue Depth| Prom["Prometheus Metrics (/metrics)"]
        Worker -->|Structured Audit Logs| DB
    end
