import inspect
from asyncio import sleep
from json import loads

from sqlalchemy.ext.asyncio import AsyncSession
from structlog import contextvars, get_logger

from message_inbox.repositories import MessageInboxRepository
from message_inbox.router import InboxRouter

logger = get_logger(__name__)


class MessageInboxProcessorWorker:
    def __init__(self, session: AsyncSession, router: InboxRouter, timeout: int = 2) -> None:
        self.session = session
        self.repository = MessageInboxRepository(session)
        self.router = router
        self.timeout = timeout

    async def process_messages(self) -> None:
        while True:
            contextvars.clear_contextvars()

            message = await self.repository.get_one()
            if message is None:
                await sleep(self.timeout)
                continue

            contextvars.bind_contextvars(message_id=str(message.id), trace_id=message.trace_id)

            logger.info("Received message")

            try:
                handler = self.router.get_handler(message.event_type)
                if handler is None:
                    logger.info("No handler for event type %s", message.event_type)
                    continue

                message_payload = loads(message.payload)

                sig = inspect.signature(handler)
                param = next(iter(sig.parameters.values()))
                annotation = param.annotation

                validated_payload = message_payload
                if inspect.isclass(annotation):
                    validated_payload = annotation(**validated_payload)

                await handler(validated_payload)
            except Exception as exc:
                logger.error("Error processing message", exc_info=True)
                raise exc
            finally:
                logger.info("Finished handling message")
                message.is_processed = True
                await self.session.commit()
