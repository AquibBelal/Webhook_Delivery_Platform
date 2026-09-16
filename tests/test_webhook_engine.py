import hmac
import hashlib
import json
import uuid
import pytest
import respx
import httpx
from sqlalchemy import select

from app.security import generate_signature
from app.models import WebhookEndpoint, WebhookDelivery, DeliveryLog
from app.worker import process_delivery
from app.queue import QUEUE_KEY, DELAYED_ZSET, DLQ_KEY
from app.config import settings

# =====================================================================
# 1. HMAC Signature Tests
# =====================================================================

def test_hmac_signature_generation():
    secret = "whsec_test_secret_key"
    payload = {"event": "order.created", "amount": 100}
    timestamp = 1710000000

    sig_header = generate_signature(secret, payload, timestamp)
    assert sig_header.startswith(f"t={timestamp},v1=")

    # Verify cryptographic validity
    sig_hash = sig_header.split("v1=")[1]
    canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected_hash = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{canonical_payload}".encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    assert hmac.compare_digest(sig_hash, expected_hash)


def test_hmac_signature_payload_key_order_invariance():
    """Signatures should match regardless of un-ordered dictionary keys."""
    secret = "whsec_test_secret_key"
    timestamp = 1710000000
    p1 = {"a": 1, "b": 2}
    p2 = {"b": 2, "a": 1}

    assert generate_signature(secret, p1, timestamp) == generate_signature(secret, p2, timestamp)


# =====================================================================
# 2. Idempotency Lock Tests
# =====================================================================

@pytest.mark.asyncio
async def test_idempotency_blocks_duplicate_requests(client, test_db_session):
    # 1. Create target endpoint
    ep_res = await client.post(
        "/endpoints",
        json={"target_url": "https://api.partner.com/webhook", "secret_token": "secret_abc"}
    )
    endpoint_id = ep_res.json()["id"]

    event_payload = {
        "endpoint_id": endpoint_id,
        "event_type": "invoice.paid",
        "payload": {"invoice_id": "inv_123"}
    }
    headers = {"Idempotency-Key": "idemp_test_unique_key_1"}

    # 2. First submission -> 202 Accepted
    first_res = await client.post("/events", json=event_payload, headers=headers)
    assert first_res.status_code == 202
    assert "delivery_id" in first_res.json()

    # 3. Duplicate submission with identical key -> 409 Conflict
    second_res = await client.post("/events", json=event_payload, headers=headers)
    assert second_res.status_code == 409
    assert "Duplicate request" in second_res.json()["detail"]


@pytest.mark.asyncio
async def test_event_ingestion_enqueues_to_redis(client, test_redis):
    ep_res = await client.post(
        "/endpoints",
        json={"target_url": "https://api.partner.com/webhook", "secret_token": "secret_abc"}
    )
    endpoint_id = ep_res.json()["id"]

    res = await client.post(
        "/events",
        json={"endpoint_id": endpoint_id, "event_type": "user.signup", "payload": {}},
        headers={"Idempotency-Key": "idemp_test_key_2"}
    )
    assert res.status_code == 202

    # Check that delivery ID is placed in Redis ready queue
    queued_id = await test_redis.lpop(QUEUE_KEY)
    assert queued_id == res.json()["delivery_id"]


# =====================================================================
# 3. Worker Processing & Retry/DLQ Logic Tests
# =====================================================================

@pytest.mark.asyncio
@respx.mock
async def test_worker_successful_delivery(test_db_session, test_redis):
    endpoint = WebhookEndpoint(
        target_url="https://client.site/webhook",
        secret_token="sign_secret"
    )
    test_db_session.add(endpoint)
    await test_db_session.commit()

    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type="payment.captured",
        payload={"amount": 5000},
        idempotency_key="idemp_success_1"
    )
    test_db_session.add(delivery)
    await test_db_session.commit()

    # Mock external endpoint 200 OK response
    respx.post("https://client.site/webhook").mock(
        return_value=httpx.Response(200, json={"received": True})
    )

    await process_delivery(str(delivery.id))

    await test_db_session.refresh(delivery)
    assert delivery.status == "SUCCESS"
    assert delivery.attempts_count == 1

    # Verify execution audit log
    log_query = await test_db_session.execute(
        select(DeliveryLog).where(DeliveryLog.delivery_id == delivery.id)
    )
    log = log_query.scalar_one()
    assert log.status_code == 200
    assert log.attempt_number == 1
    assert log.error_message is None


@pytest.mark.asyncio
@respx.mock
async def test_worker_transient_failure_exponential_backoff(test_db_session, test_redis):
    endpoint = WebhookEndpoint(
        target_url="https://client.site/failing",
        secret_token="sign_secret"
    )
    test_db_session.add(endpoint)
    await test_db_session.commit()

    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type="order.failed",
        payload={},
        idempotency_key="idemp_retry_1",
        attempts_count=0
    )
    test_db_session.add(delivery)
    await test_db_session.commit()

    # Mock 503 Service Unavailable
    respx.post("https://client.site/failing").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )

    await process_delivery(str(delivery.id))

    await test_db_session.refresh(delivery)
    assert delivery.status == "RETRYING"
    assert delivery.attempts_count == 1

    # Verify task placed in delayed ZSET with future timestamp
    delayed_jobs = await test_redis.zrangebyscore(DELAYED_ZSET, 0, "+inf", withscores=True)
    assert len(delayed_jobs) == 1
    job_id, execute_at = delayed_jobs[0]
    assert job_id == str(delivery.id)
    assert execute_at > 0


@pytest.mark.asyncio
@respx.mock
async def test_worker_exceeding_max_retries_moves_to_dlq(test_db_session, test_redis):
    endpoint = WebhookEndpoint(
        target_url="https://client.site/timeout",
        secret_token="sign_secret"
    )
    test_db_session.add(endpoint)
    await test_db_session.commit()

    # Delivery already at 4 attempts (max is 5)
    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type="payout.created",
        payload={},
        idempotency_key="idemp_dlq_1",
        attempts_count=settings.MAX_RETRIES - 1
    )
    test_db_session.add(delivery)
    await test_db_session.commit()

    # Mock HTTP connection timeout
    respx.post("https://client.site/timeout").mock(
        side_effect=httpx.ConnectTimeout("Connection timed out")
    )

    await process_delivery(str(delivery.id))

    await test_db_session.refresh(delivery)
    assert delivery.status == "DLQ"
    assert delivery.attempts_count == settings.MAX_RETRIES

    # Verify delivery ID moved to Redis DLQ list
    dlq_item = await test_redis.lpop(DLQ_KEY)
    assert dlq_item == str(delivery.id)


# =====================================================================
# 4. DLQ Replay Endpoint Tests
# =====================================================================

@pytest.mark.asyncio
async def test_dlq_replay_endpoint(client, test_db_session, test_redis):
    endpoint = WebhookEndpoint(
        target_url="https://client.site/recovered",
        secret_token="secret"
    )
    test_db_session.add(endpoint)
    await test_db_session.commit()

    delivery = WebhookDelivery(
        endpoint_id=endpoint.id,
        event_type="retry.replay",
        payload={},
        idempotency_key="idemp_replay_1",
        status="DLQ",
        attempts_count=5
    )
    test_db_session.add(delivery)
    await test_db_session.commit()

    # Push to DLQ list manually
    await test_redis.rpush(DLQ_KEY, str(delivery.id))

    # Trigger replay
    response = await client.post("/dlq/replay")
    assert response.status_code == 200
    assert response.json() == {"replayed_count": 1}

    # Verify status reset in DB
    await test_db_session.refresh(delivery)
    assert delivery.status == "PENDING"
    assert delivery.attempts_count == 0

    # Verify pushed back into ready queue
    re_queued = await test_redis.lpop(QUEUE_KEY)
    assert re_queued == str(delivery.id)