import inspect
from asyncio import sleep
from contextvars import ContextVar
from json import loads

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars, get_logger

from message_inbox.repositories import MessageInboxRepository
from message_inbox.router import InboxRouter

logger = get_logger(__name__)

context_trace_id: ContextVar[str | None] = ContextVar("trace_id")


class MessageInboxProcessorWorker:
    def __init__(
        self, session_maker: async_sessionmaker[AsyncSession], router: InboxRouter, timeout: int = 2
    ) -> None:
        self.session_maker = session_maker
        self.router = router
        self.timeout = timeout

    async def process_messages(self) -> None:
        while True:
            contextvars.clear_contextvars()

            async with self.session_maker() as session:
                repository = MessageInboxRepository(session)

                message = await repository.get_one()
                if message is None:
                    await sleep(self.timeout)
                    continue

                contextvars.bind_contextvars(message_id=str(message.id), trace_id=message.trace_id)
                context_trace_id.set(message.trace_id)

                logger.info("Received message")

                try:
                    handler = self.router.get_handler(message.event_type)
                    if handler is None:
                        logger.info("No handler for event type %s", message.event_type)
                        continue

                    sig = inspect.signature(handler)
                    param = next(iter(sig.parameters.values()))
                    annotation = param.annotation

                    payload = message.payload
                    if inspect.isclass(annotation):
                        payload = annotation(**payload)

                    await handler(payload, session)
                except Exception as exc:
                    logger.error("Error processing message", exc_info=True)
                    raise exc
                finally:
                    logger.info("Finished handling message")
                    message.is_processed = True
                    await session.commit()
