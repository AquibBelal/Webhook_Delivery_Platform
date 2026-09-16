from prometheus_client import Counter, Histogram, Gauge

DELIVERY_ATTEMPTS = Counter(
    "webhook_delivery_attempts_total",
    "Total webhook delivery attempts by outcome",
    ["status"]
)

DELIVERY_LATENCY = Histogram(
    "webhook_delivery_duration_seconds",
    "Latency of outbound webhook calls in seconds",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

QUEUE_DEPTH = Gauge(
    "webhook_queue_depth",
    "Current depth of webhook queues",
    ["queue_name"]
)