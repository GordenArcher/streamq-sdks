# Public API surface for the streamq Python SDK.
#
# Everything a caller needs is importable directly from `streamq`:
#
#   from streamq import AsyncClient, SyncClient
#   from streamq import StreamqError, StreamqNotFound
#   from streamq import Message, TopicInfo, PublishResult
#
# Internal modules (async_client, consumer, sync, models, exceptions)
# are not part of the public API — callers should not import from them
# directly, as their internal structure may change between versions.

from .async_client import AsyncClient
from .consumer import AsyncConsumer
from .exceptions import (
    StreamqAPIError,
    StreamqConflict,
    StreamqConsumerClosed,
    StreamqError,
    StreamqMaxReconnects,
    StreamqNotFound,
)
from .models import Message, PublishResult, TopicInfo
from .sync import SyncClient, SyncConsumer

__all__ = [
    # Clients
    "AsyncClient",
    "SyncClient",
    # Consumers
    "AsyncConsumer",
    "SyncConsumer",
    # Models
    "Message",
    "TopicInfo",
    "PublishResult",
    # Exceptions
    "StreamqError",
    "StreamqNotFound",
    "StreamqConflict",
    "StreamqAPIError",
    "StreamqConsumerClosed",
    "StreamqMaxReconnects",
]

__version__ = "0.1.0"
