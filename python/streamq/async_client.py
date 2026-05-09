# Async client for the streamq broker API.
#
# This is the primary client for applications already using asyncio
# (FastAPI, aiohttp servers, async Django, etc.). It uses aiohttp for
# HTTP requests and shares a single ClientSession across all calls for
# connection pooling.
#
# Usage:
#
#   async with AsyncClient("http://localhost:8080") as client:
#       await client.create_topic("payments")
#       result = await client.publish("payments", b'{"amount": 100}')
#
#       async with client.subscribe("payments", group="billing") as consumer:
#           async for msg in consumer:
#               print(msg.payload)
#               await consumer.ack(msg.offset)
#
# Thread safety:
# AsyncClient is NOT thread-safe. Use it from a single asyncio event loop.
# For multi-threaded or sync use, see SyncClient in sync.py.

from __future__ import annotations

import base64
import logging
from typing import List, Optional

import aiohttp

from .consumer import AsyncConsumer
from .exceptions import StreamqAPIError, StreamqConflict, StreamqNotFound
from .models import PublishResult, TopicInfo

logger = logging.getLogger(__name__)


class AsyncClient:
    """Async client for the streamq broker.

    Use as an async context manager to ensure the underlying aiohttp
    session is properly closed:

        async with AsyncClient("http://localhost:8080") as client:
            ...

    Or manage the lifecycle manually:

        client = AsyncClient("http://localhost:8080")
        await client.open()
        ...
        await client.close()
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        reconnect_delay: float = 2.0,
        max_reconnect_attempts: int = 0,
    ) -> None:
        """
        Args:
            base_url: Broker address, e.g. "http://localhost:8080".
            timeout: Timeout in seconds for individual HTTP API calls.
                     Does not apply to streaming connections.
            reconnect_delay: Seconds to wait between consumer reconnect attempts.
            max_reconnect_attempts: Max consumer reconnects before giving up.
                                    0 = retry forever.
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._session: Optional[aiohttp.ClientSession] = None

    async def open(self) -> None:
        """Open the underlying HTTP session. Called automatically by __aenter__."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)

    async def close(self) -> None:
        """Close the underlying HTTP session and release connections."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "AsyncClient":
        await self.open()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def create_topic(self, name: str) -> None:
        """Create a new topic on the broker.

        Raises:
            StreamqConflict: Topic already exists.
            StreamqAPIError: Any other broker error.
        """
        await self._request("POST", "/topics", json={"name": name}, expect=201)

    async def delete_topic(self, name: str) -> None:
        """Delete a topic and disconnect all its subscribers.

        Raises:
            StreamqNotFound: Topic does not exist.
            StreamqAPIError: Any other broker error.
        """
        await self._request("DELETE", f"/topics/{name}", expect=200)

    async def list_topics(self) -> List[TopicInfo]:
        """Return stats for all topics on the broker.

        Returns an empty list if no topics exist.
        """
        data = await self._request("GET", "/topics", expect=200)
        return [TopicInfo.from_dict(t) for t in (data.get("data") or [])]

    async def get_topic(self, name: str) -> TopicInfo:
        """Return stats for a single topic.

        Raises:
            StreamqNotFound: Topic does not exist.
        """
        data = await self._request("GET", f"/topics/{name}", expect=200)
        return TopicInfo.from_dict(data.get("data", {}))

    async def publish(self, topic: str, payload: bytes) -> PublishResult:
        """Publish a message to the named topic.

        payload is sent as base64 in the JSON request body — the broker
        treats it as opaque bytes.

        Returns:
            PublishResult with the assigned offset and server timestamp.

        Raises:
            StreamqNotFound: Topic does not exist.
            StreamqAPIError: Any other broker error.
        """
        # base64-encode payload for JSON transport.
        # The broker's publish endpoint expects: {"payload": "<base64>"}
        encoded = base64.b64encode(payload).decode("ascii")
        data = await self._request(
            "POST",
            f"/topics/{topic}/publish",
            json={"payload": encoded},
            expect=202,
        )
        return PublishResult.from_dict(data.get("data", {}))

    def subscribe(
        self,
        topic: str,
        *,
        group: str = "",
        from_offset: int = -1,
        protocol: str = "ws",
        buffer_size: int = 64,
    ) -> AsyncConsumer:
        """Create an AsyncConsumer for the given topic.

        Returns immediately — the connection is established lazily when
        iteration begins. Use as an async context manager or async iterator.

        Args:
            topic: Topic name to subscribe to.
            group: Consumer group ID. Empty = broadcast (receive all messages).
            from_offset: Starting offset. -1 = tail (new messages only).
                         0 = full history replay. N = resume from offset N.
            protocol: "ws" (WebSocket, supports ack) or "sse" (SSE, simpler).
            buffer_size: Internal message buffer size.

        Returns:
            AsyncConsumer — async iterable that yields Message objects.

        Example::

            async with client.subscribe("payments", group="billing") as consumer:
                async for msg in consumer:
                    process(msg.payload)
                    await consumer.ack(msg.offset)
        """
        return AsyncConsumer(
            self._base_url,
            topic,
            group=group,
            from_offset=from_offset,
            protocol=protocol,
            buffer_size=buffer_size,
            reconnect_delay=self._reconnect_delay,
            max_reconnect_attempts=self._max_reconnect_attempts,
            session=self._session,  # share the client's session
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict] = None,
        expect: int,
    ) -> dict:
        """Make an HTTP request to the broker and return the parsed JSON body.

        Raises the appropriate SDK exception for non-2xx responses.
        """
        if self._session is None or self._session.closed:
            await self.open()

        url = self._base_url + path

        async with self._session.request(method, url, json=json) as resp:
            body = await resp.json(content_type=None)

            if resp.status != expect:
                message = body.get("message", f"HTTP {resp.status}")
                if resp.status == 404:
                    raise StreamqNotFound(message)
                elif resp.status == 409:
                    raise StreamqConflict(message)
                else:
                    raise StreamqAPIError(resp.status, message)

            return body
