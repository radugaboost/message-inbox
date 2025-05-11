from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from message_inbox.models import MessageInbox
from message_inbox.schemas import MessageSchema


@dataclass
class MessageInboxRepository:
    session: AsyncSession

    async def get_by_id(self, message_id: UUID) -> MessageInbox | None:
        return (
            await self.session.scalars(select(MessageInbox).where(MessageInbox.id == message_id))
        ).one_or_none()

    async def create(self, message: MessageSchema) -> MessageInbox:
        new_message = MessageInbox(**message.model_dump())
        self.session.add(new_message)

        return new_message

    async def get_one(self) -> MessageInbox | None:
        return (
            await self.session.scalars(
                select(MessageInbox)
                .where(MessageInbox.is_processed.is_(False))
                .order_by(MessageInbox.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
        ).one_or_none()

    async def batch_mark_processed(self, ids: list[UUID]) -> None:
        await self.session.execute(
            update(MessageInbox).where(MessageInbox.id.in_(ids)).values(is_processed=True)
        )
