import uuid
from datetime import datetime
from pydantic import BaseModel, HttpUrl

class EndpointCreate(BaseModel):
    target_url: HttpUrl
    secret_token: str

class EndpointResponse(BaseModel):
    id: uuid.UUID
    target_url: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class EventPublish(BaseModel):
    endpoint_id: uuid.UUID
    event_type: str
    payload: dict

class DeliveryLogResponse(BaseModel):
    attempt_number: int
    status_code: int | None
    latency_ms: int | None
    error_message: str | None
    attempted_at: datetime

    class Config:
        from_attributes = True

class DeliveryStatusResponse(BaseModel):
    id: uuid.UUID
    endpoint_id: uuid.UUID
    event_type: str
    status: str
    attempts_count: int
    created_at: datetime
    updated_at: datetime
    logs: list[DeliveryLogResponse] = []

    class Config:
        from_attributes = True