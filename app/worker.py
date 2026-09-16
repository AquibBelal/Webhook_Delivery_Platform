import asyncio
import time
import random
import httpx
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import WebhookDelivery, WebhookEndpoint, DeliveryLog
from app.queue import redis_client, QUEUE_KEY, DELAYED_ZSET, DLQ_KEY, schedule_retry, move_to_dlq
from app.security import generate_signature
from app.config import settings
from app.metrics import DELIVERY_ATTEMPTS, DELIVERY_LATENCY

async def process_delivery(delivery_id_str: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WebhookDelivery).where(WebhookDelivery.id == delivery_id_str)
        )
        delivery = result.scalar_one_or_none()
        if not delivery:
            return

        endpoint_res = await session.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.id == delivery.endpoint_id)
        )
        endpoint = endpoint_res.scalar_one_or_none()
        if not endpoint or not endpoint.is_active:
            delivery.status = "FAILED"
            await session.commit()
            return

        delivery.attempts_count += 1
        current_attempt = delivery.attempts_count
        timestamp = int(time.time())
        signature = generate_signature(endpoint.secret_token, delivery.payload, timestamp)

        headers = {
            "Content-Type": "application/json",
            "X-Signature-SHA256": signature,
            "X-Delivery-Attempt": str(current_attempt),
            "X-Event-Type": delivery.event_type,
        }

        status_code = None
        response_body = None
        error_msg = None
        latency_ms = None
        is_success = False

        start_time = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT_SECONDS) as client:
                res = await client.post(endpoint.target_url, json=delivery.payload, headers=headers)
                latency_ms = int((time.perf_counter() - start_time) * 1000)
                status_code = res.status_code
                response_body = res.text[:1000]
                is_success = 200 <= status_code < 300
        except Exception as exc:
            latency_ms = int((time.perf_counter() - start_time) * 1000)
            error_msg = str(exc)

        # Log attempt
        log = DeliveryLog(
            delivery_id=delivery.id,
            attempt_number=current_attempt,
            status_code=status_code,
            response_body=response_body,
            latency_ms=latency_ms,
            error_message=error_msg,
        )
        session.add(log)

        # Monitoring :
        if is_success:
            DELIVERY_ATTEMPTS.labels(status="2xx").inc()
        elif status_code and 400 <= status_code < 500:
            DELIVERY_ATTEMPTS.labels(status="4xx").inc()
        elif status_code and status_code >= 500:
            DELIVERY_ATTEMPTS.labels(status="5xx").inc()
        else:
            DELIVERY_ATTEMPTS.labels(status="timeout_or_network_error").inc()

        if latency_ms is not None:
            DELIVERY_LATENCY.observe(latency_ms / 1000.0)

        if is_success:
            delivery.status = "SUCCESS"
            await session.commit()
        else:
            if current_attempt < settings.MAX_RETRIES:
                # Exponential backoff with jitter
                backoff = (settings.INITIAL_BACKOFF_SECONDS ** current_attempt) + random.uniform(0.1, 1.0)
                run_at = time.time() + backoff
                delivery.status = "RETRYING"
                await session.commit()
                await schedule_retry(str(delivery.id), run_at)
            else:
                delivery.status = "DLQ"
                await session.commit()
                await move_to_dlq(str(delivery.id))

async def poll_delayed_jobs():
    """Moves expired retry jobs from Redis ZSET to ready queue."""
    while True:
        try:
            now = time.time()
            ready_jobs = await redis_client.zrangebyscore(DELAYED_ZSET, 0, now)
            if ready_jobs:
                pipe = redis_client.pipeline()
                for job_id in ready_jobs:
                    pipe.zrem(DELAYED_ZSET, job_id)
                    pipe.rpush(QUEUE_KEY, job_id)
                await pipe.execute()
        except Exception as e:
            print(f"Error in delayed scheduler: {e}")
        await asyncio.sleep(1)

async def run_worker():
    """Continuously processes jobs from the ready queue."""
    asyncio.create_task(poll_delayed_jobs())
    print("Worker engine started. Listening for events...")
    while True:
        try:
            item = await redis_client.blpop(QUEUE_KEY, timeout=2)
            if item:
                _, delivery_id = item
                await process_delivery(delivery_id)
        except Exception as e:
            print(f"Error processing delivery job: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(run_worker())