# Async consumer for streamq topics.
#
# Design — async generator pattern:
# The consumer exposes an async generator via __aiter__ / __anext__.
# This is the most Pythonic way to consume a stream:
#
#   async for msg in consumer:
#       await process(msg)
#       await consumer.ack(msg.offset)
#
# Why async generator instead of asyncio.Queue?
# A queue-based API requires the caller to manage a background task and
# remember to cancel it. The async generator pattern handles all lifecycle
# internally — the generator runs until the connection drops, then
# reconnects transparently. Cleanup happens in the finally block.
#
# Reconnect behaviour:
# On connection drop, the consumer waits reconnect_delay seconds and retries.
# For grouped consumers with a committed offset, the broker resumes from
# where the group left off automatically. If max_reconnect_attempts is
# exceeded, StreamqMaxReconnects is raised from the generator.
#
# Protocol support:
# - WebSocket: bidirectional, supports explicit ack() for at-least-once delivery
# - SSE: unidirectional, at-most-once (no ack channel)
#
# Thread safety:
# The consumer is not thread-safe. Use it from a single asyncio event loop.
# For multi-threaded use, see SyncConsumer in sync.py.

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator, Optional
from urllib.parse import urlencode

import aiohttp

from .exceptions import StreamqConsumerClosed, StreamqMaxReconnects
from .models import Message

logger = logging.getLogger(__name__)


class AsyncConsumer:
    """Async consumer for a streamq topic.

    Obtain via AsyncClient.subscribe(). Use as an async context manager
    or async iterator:

        async with client.subscribe("payments") as consumer:
            async for msg in consumer:
                print(msg.payload)
                await consumer.ack(msg.offset)

    Or without context manager:

        consumer = client.subscribe("payments")
        async for msg in consumer:
            ...
        await consumer.close()
    """

    def __init__(
        self,
        base_url: str,
        topic: str,
        *,
        group: str = "",
        from_offset: int = -1,
        protocol: str = "ws",  # "ws" or "sse"
        buffer_size: int = 64,
        reconnect_delay: float = 2.0,
        max_reconnect_attempts: int = 0,  # 0 = unlimited
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._topic = topic
        self._group = group
        self._from_offset = from_offset
        self._protocol = protocol
        self._buffer_size = buffer_size
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts

        # If a session is passed in (from AsyncClient), we don't own it
        # and should not close it. If we create our own, we close it on exit.
        self._session = session
        self._owns_session = session is None

        self._closed = False
        self._close_event = asyncio.Event()

        # ack_queue carries offset values from ack() to the WebSocket write loop.
        # Buffered so ack() never blocks.
        self._ack_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=buffer_size)

    async def __aenter__(self) -> "AsyncConsumer":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[Message]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[Message]:
        """Main generator — connects, yields messages, reconnects on failure."""
        if self._owns_session:
            self._session = aiohttp.ClientSession()

        attempts = 0
        last_error: Optional[Exception] = None

        try:
            while not self._closed:
                try:
                    if self._protocol == "sse":
                        async for msg in self._connect_sse():
                            yield msg
                    else:
                        async for msg in self._connect_ws():
                            yield msg

                    # Generator exhausted cleanly (broker closed the connection).
                    # If we weren't explicitly closed, reconnect.
                    if self._closed:
                        return

                except StreamqConsumerClosed:
                    return

                except asyncio.CancelledError:
                    return

                except Exception as e:
                    last_error = e
                    attempts += 1
                    logger.warning(
                        "streamq consumer: connection lost (%s), attempt %d",
                        e,
                        attempts,
                    )

                    if (
                        self._max_reconnect_attempts > 0
                        and attempts >= self._max_reconnect_attempts
                    ):
                        raise StreamqMaxReconnects(attempts, last_error)

                    # Wait before reconnecting. Respect close() during wait.
                    try:
                        await asyncio.wait_for(
                            self._close_event.wait(),
                            timeout=self._reconnect_delay,
                        )
                        # close_event fired — stop.
                        return
                    except asyncio.TimeoutError:
                        pass  # reconnect delay elapsed — try again

        finally:
            if self._owns_session and self._session:
                await self._session.close()
                self._session = None

    async def _connect_ws(self) -> AsyncIterator[Message]:
        """Connect via WebSocket and yield messages until disconnected."""
        url = self._build_url("ws")
        logger.debug("streamq: connecting WebSocket to %s", url)

        async with self._session.ws_connect(url) as ws:
            # Launch write pump as a task, sends ack frames to the broker.
            # We use a task (not gather) so the read loop can yield independently.
            write_task = asyncio.create_task(self._ws_write_pump(ws))

            try:
                async for raw in ws:
                    if raw.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(raw.data)
                            msg = Message.from_dict(data)
                            yield msg
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning("streamq: malformed ws frame: %s", e)
                            continue

                    elif raw.type == aiohttp.WSMsgType.CLOSE:
                        break

                    elif raw.type == aiohttp.WSMsgType.ERROR:
                        raise aiohttp.ClientError(f"ws error: {ws.exception()}")
            finally:
                write_task.cancel()
                try:
                    await write_task
                except asyncio.CancelledError:
                    pass

    async def _ws_write_pump(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send ack frames from the ack queue to the broker.

        Runs as a background task while the read loop is active.
        Cancelled when the read loop exits.
        """
        while True:
            offset = await self._ack_queue.get()
            frame = json.dumps({"ack": offset})
            try:
                await ws.send_str(frame)
            except Exception as e:
                logger.warning(
                    "streamq: failed to send ack for offset %d: %s", offset, e
                )

    async def _connect_sse(self) -> AsyncIterator[Message]:
        """Connect via SSE and yield messages until disconnected."""
        url = self._build_url("sse")
        logger.debug("streamq: connecting SSE to %s", url)

        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }

        async with self._session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                )

            # Parse SSE line by line.
            # SSE format:
            #   data: <json>
            #   <blank line>  ← event boundary
            data_line = ""

            async for line_bytes in resp.content:
                if self._closed:
                    return

                line = line_bytes.decode("utf-8").rstrip("\n\r")

                if line.startswith("data: "):
                    data_line = line[6:]  # strip "data: " prefix

                elif line == "" and data_line:
                    # Blank line = end of event.
                    try:
                        data = json.loads(data_line)
                        msg = Message.from_dict(data)
                        yield msg
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning("streamq: malformed sse event: %s", e)
                    finally:
                        data_line = ""

    async def ack(self, offset: int) -> None:
        """Acknowledge a message offset.

        Only meaningful for WebSocket consumers in a consumer group.
        The broker advances the group's committed offset on receipt,
        providing at-least-once delivery guarantees.

        For SSE consumers or ungrouped consumers, this is a no-op.
        """
        if self._protocol == "sse" or not self._group:
            return

        try:
            self._ack_queue.put_nowait(offset)
        except asyncio.QueueFull:
            # Ack queue full, shouldn't happen under normal operation.
            # Drop silently; worst case is redelivery on reconnect.
            logger.warning(
                "streamq: ack queue full, dropping ack for offset %d", offset
            )

    async def close(self) -> None:
        """Stop the consumer and release resources.

        Idempotent — safe to call multiple times.
        After close(), the async iterator will stop yielding.
        """
        if not self._closed:
            self._closed = True
            self._close_event.set()

    def _build_url(self, protocol: str) -> str:
        """Build the subscription URL for the given protocol.

        WebSocket: ws://host/topics/<name>/subscribe/ws?group=X&from_offset=Y
        SSE:       http://host/topics/<name>/subscribe/sse?group=X&from_offset=Y
        """
        base = self._base_url

        if protocol == "ws":
            # Replace http(s) scheme with ws(s) for WebSocket URLs.
            base = base.replace("https://", "wss://").replace("http://", "ws://")

        path = f"{base}/topics/{self._topic}/subscribe/{protocol}"

        params: dict[str, str] = {}
        if self._group:
            params["group"] = self._group
        if self._from_offset >= 0:
            params["from_offset"] = str(self._from_offset)

        if params:
            return f"{path}?{urlencode(params)}"
        return path
