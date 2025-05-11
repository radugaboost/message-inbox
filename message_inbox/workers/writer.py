from json import JSONDecodeError, loads
from typing import Any

from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars, get_logger

from message_inbox.repositories import MessageInboxRepository
from message_inbox.schemas import KafkaMessageValueSchema, MessageSchema

logger = get_logger(__name__)


class MessageInboxWriterWorker:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession], consumer: AIOKafkaConsumer):
        self.session_maker = session_maker
        self.consumer = consumer

    async def start_consuming(self) -> None:
        logger.info("Starting consuming")

        async for message in self.consumer:
            contextvars.clear_contextvars()

            logger.info("Received message", message=message)

            try:
                message_value = loads(message.value)
            except JSONDecodeError:
                logger.error("Decode error", exc_info=True)
                continue

            headers = self.parse_headers(message.headers)
            message_id = headers.get("x-message-id")
            trace_id = headers.get("x-trace-id")

            if message_id is None:
                logger.error("No message id")
                continue

            contextvars.bind_contextvars(
                message_id=message_id,
                trace_id=trace_id,
            )

            async with self.session_maker() as session:
                repository = MessageInboxRepository(session)

                db_message = await repository.get_by_id(message_id)
                if db_message is not None:
                    logger.info("The message has already been received", message_id=message_id)
                    continue

                validated_message_value = KafkaMessageValueSchema(**message_value)
                validated_message = MessageSchema(
                    id=message_id,
                    topic=message.topic,
                    trace_id=trace_id,
                    payload=validated_message_value.payload,
                    event_type=validated_message_value.event_type,
                )
                await repository.create(validated_message)
                await session.commit()

                logger.info("The message has been written to the inbox", message_id=message_id)

    @staticmethod
    def parse_headers(headers: list[tuple[str, bytes]]) -> dict[str, Any]:
        parsed_headers = {}

        for header in headers:
            parsed_headers[header[0]] = header[1].decode("utf-8")

        return parsed_headers
