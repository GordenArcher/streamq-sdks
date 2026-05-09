# Synchronous client for the streamq broker.
#
# Why a sync wrapper?
# Most Python backend code in the wild is still synchronous — Django views,
# Flask routes, plain scripts, Celery tasks. Making the primary API async
# would exclude all of these callers unless they wrap everything in
# asyncio.run() themselves, which is error-prone and repetitive.
#
# Design — run_in_executor pattern:
# SyncClient wraps AsyncClient and runs every async call on a dedicated
# background event loop running in a separate thread. This means:
#   - No asyncio.run() overhead per call (the loop is reused)
#   - No interference with any existing event loop in the caller's thread
#   - Thread-safe: the background loop thread is the only one touching
#     the aiohttp session
#
# SyncConsumer wraps AsyncConsumer and exposes a regular Python iterator.
# Calling next() blocks until the next message arrives or the consumer stops.
#
# Usage:
#
#   client = SyncClient("http://localhost:8080")
#
#   client.create_topic("payments")
#   result = client.publish("payments", b'{"amount": 100}')
#
#   with client.subscribe("payments", group="billing") as consumer:
#       for msg in consumer:
#           process(msg.payload)
#           consumer.ack(msg.offset)
#
#   client.close()

from __future__ import annotations

import asyncio
import queue
import threading
from typing import Iterator, List, Optional

from .async_client import AsyncClient
from .consumer import AsyncConsumer
from .exceptions import StreamqConsumerClosed
from .models import Message, PublishResult, TopicInfo

# Sentinel value used to signal the sync iterator that the async stream ended.
_DONE = object()
_ERROR = object()


class _BackgroundLoop:
    """A background thread running a dedicated asyncio event loop.

    Shared across all SyncClient instances created without an explicit loop.
    Reusing the loop avoids the overhead of creating a new thread per client.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,  # daemon so it doesn't block process exit
            name="streamq-event-loop",
        )
        self._thread.start()

    def _run(self) -> None:
        self._loop.run_forever()

    def run(self, coro):
        """Submit a coroutine to the background loop and block until done."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


# Module-level shared background loop.
# Created lazily on first SyncClient instantiation.
_shared_loop: Optional[_BackgroundLoop] = None
_loop_lock = threading.Lock()


def _get_shared_loop() -> _BackgroundLoop:
    global _shared_loop
    with _loop_lock:
        if _shared_loop is None:
            _shared_loop = _BackgroundLoop()
        return _shared_loop


class SyncConsumer:
    """Synchronous iterator over streamq messages.

    Obtain via SyncClient.subscribe(). Use as a context manager:

        with client.subscribe("payments", group="billing") as consumer:
            for msg in consumer:
                process(msg.payload)
                consumer.ack(msg.offset)
    """

    def __init__(self, async_consumer: AsyncConsumer, loop: _BackgroundLoop) -> None:
        self._async_consumer = async_consumer
        self._loop = loop
        self._queue: queue.Queue = queue.Queue(maxsize=64)
        self._pump_task: Optional[asyncio.Future] = None
        self._start_pump()

    def _start_pump(self) -> None:
        """Start a background task that feeds messages from the async consumer
        into the sync queue. The main thread reads from the queue."""

        async def pump():
            try:
                async for msg in self._async_consumer:
                    self._queue.put(msg)
                self._queue.put(_DONE)
            except (StreamqConsumerClosed, asyncio.CancelledError):
                self._queue.put(_DONE)
            except Exception as e:
                # Put the error in the queue so __next__ can raise it.
                self._queue.put((_ERROR, e))

        future = asyncio.run_coroutine_threadsafe(pump(), self._loop._loop)
        self._pump_task = future

    def __enter__(self) -> "SyncConsumer":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __iter__(self) -> Iterator[Message]:
        return self

    def __next__(self) -> Message:
        item = self._queue.get()

        if item is _DONE:
            raise StopIteration

        if isinstance(item, tuple) and len(item) == 2 and item[0] is _ERROR:
            raise item[1]

        return item

    def ack(self, offset: int) -> None:
        """Acknowledge a message offset (WebSocket + group consumers only)."""
        asyncio.run_coroutine_threadsafe(
            self._async_consumer.ack(offset),
            self._loop._loop,
        )

    def close(self) -> None:
        """Stop the consumer. Idempotent."""
        asyncio.run_coroutine_threadsafe(
            self._async_consumer.close(),
            self._loop._loop,
        )


class SyncClient:
    """Synchronous client for the streamq broker.

    Wraps AsyncClient and runs all async operations on a dedicated background
    event loop thread. Safe to use from synchronous Django/Flask/Celery code.

    Usage::

        client = SyncClient("http://localhost:8080")
        client.create_topic("payments")
        result = client.publish("payments", b'{"amount": 100}')

        with client.subscribe("payments", group="billing") as consumer:
            for msg in consumer:
                process(msg.payload)
                consumer.ack(msg.offset)

        client.close()
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        reconnect_delay: float = 2.0,
        max_reconnect_attempts: int = 0,
    ) -> None:
        self._loop = _get_shared_loop()
        self._async_client = AsyncClient(
            base_url,
            timeout=timeout,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnect_attempts,
        )
        # Open the underlying aiohttp session on the background loop.
        self._loop.run(self._async_client.open())

    def close(self) -> None:
        """Close the client and release resources."""
        self._loop.run(self._async_client.close())

    def __enter__(self) -> "SyncClient":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def create_topic(self, name: str) -> None:
        """Create a new topic. Raises StreamqConflict if already exists."""
        self._loop.run(self._async_client.create_topic(name))

    def delete_topic(self, name: str) -> None:
        """Delete a topic. Raises StreamqNotFound if not found."""
        self._loop.run(self._async_client.delete_topic(name))

    def list_topics(self) -> List[TopicInfo]:
        """Return stats for all topics."""
        return self._loop.run(self._async_client.list_topics())

    def get_topic(self, name: str) -> TopicInfo:
        """Return stats for a single topic."""
        return self._loop.run(self._async_client.get_topic(name))

    def publish(self, topic: str, payload: bytes) -> PublishResult:
        """Publish a message. Returns PublishResult with offset and timestamp."""
        return self._loop.run(self._async_client.publish(topic, payload))

    def subscribe(
        self,
        topic: str,
        *,
        group: str = "",
        from_offset: int = -1,
        protocol: str = "ws",
        buffer_size: int = 64,
    ) -> SyncConsumer:
        """Create a SyncConsumer for the given topic.

        Use as a context manager::

            with client.subscribe("payments", group="billing") as consumer:
                for msg in consumer:
                    process(msg.payload)
                    consumer.ack(msg.offset)
        """
        async_consumer = self._async_client.subscribe(
            topic,
            group=group,
            from_offset=from_offset,
            protocol=protocol,
            buffer_size=buffer_size,
        )
        return SyncConsumer(async_consumer, self._loop)
