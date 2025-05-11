from uuid import UUID

from pydantic import BaseModel, ConfigDict


class BaseSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)


class MessageSchema(BaseSchema):
    id: UUID
    topic: str
    event_type: str
    payload: str
    trace_id: str | None = None


class KafkaMessageValueSchema(BaseSchema):
    event_type: str
    payload: str
