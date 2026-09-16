import json
import redis.asyncio as aioredis
from app.config import settings

redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

QUEUE_KEY = "webhook:queue:ready"
DLQ_KEY = "webhook:queue:dlq"
DELAYED_ZSET = "webhook:queue:delayed"

async def enqueue_delivery(delivery_id: str):
    await redis_client.rpush(QUEUE_KEY, delivery_id)

async def schedule_retry(delivery_id: str, run_at_timestamp: float):
    await redis_client.zadd(DELAYED_ZSET, {delivery_id: run_at_timestamp})

async def move_to_dlq(delivery_id: str):
    await redis_client.rpush(DLQ_KEY, delivery_id)