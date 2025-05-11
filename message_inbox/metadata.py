from sqlalchemy import MetaData

from message_inbox.models import Base


def get_metadata() -> MetaData:
    return Base.metadata
