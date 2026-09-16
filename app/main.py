import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Header, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.database import engine, Base, get_db
from app.models import WebhookEndpoint, WebhookDelivery
from app.schemas import (
    EndpointCreate, EndpointResponse, EventPublish, DeliveryStatusResponse
)
from app.queue import enqueue_delivery, redis_client, DLQ_KEY, QUEUE_KEY
from fastapi import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    title="Webhook Delivery Platform",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/endpoints", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def register_endpoint(payload: EndpointCreate, db: AsyncSession = Depends(get_db)):
    endpoint = WebhookEndpoint(
        target_url=str(payload.target_url),
        secret_token=payload.secret_token
    )
    db.add(endpoint)
    await db.commit()
    await db.refresh(endpoint)
    return endpoint

@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def publish_event(
    event: EventPublish,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db)
):
    # Distributed idempotency lock via Redis
    lock_acquired = await redis_client.set(f"idempotency:{idempotency_key}", "locked", nx=True, ex=86400)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Duplicate request: Idempotency-Key already submitted."
        )

    # Validate endpoint exists
    result = await db.execute(select(WebhookEndpoint).where(WebhookEndpoint.id == event.endpoint_id))
    endpoint = result.scalar_one_or_none()
    if not endpoint or not endpoint.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target endpoint not found or inactive.")

    delivery = WebhookDelivery(
        endpoint_id=event.endpoint_id,
        event_type=event.event_type,
        payload=event.payload,
        idempotency_key=idempotency_key,
        status="PENDING"
    )
    db.add(delivery)
    await db.commit()
    await db.refresh(delivery)

    await enqueue_delivery(str(delivery.id))
    return {"delivery_id": delivery.id, "status": "PENDING"}

@app.get("/deliveries/{delivery_id}", response_model=DeliveryStatusResponse)
async def get_delivery_status(delivery_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(WebhookDelivery)
        .options(selectinload(WebhookDelivery.logs))
        .where(WebhookDelivery.id == delivery_id)
    )
    delivery = result.scalar_one_or_none()
    if not delivery:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Delivery record not found.")
    return delivery

@app.post("/dlq/replay", status_code=status.HTTP_200_OK)
async def replay_dlq(db: AsyncSession = Depends(get_db)):
    """Drain all items from the DLQ and re-enqueue them."""
    replayed_count = 0
    while True:
        delivery_id = await redis_client.lpop(DLQ_KEY)
        if not delivery_id:
            break

        result = await db.execute(select(WebhookDelivery).where(WebhookDelivery.id == delivery_id))
        delivery = result.scalar_one_or_none()
        if delivery:
            delivery.status = "PENDING"
            delivery.attempts_count = 0
            await db.commit()
            await enqueue_delivery(delivery_id)
            replayed_count += 1

    return {"replayed_count": replayed_count}

@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)