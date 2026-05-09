# Tests for AsyncClient using aiohttp's built-in test server.
# No real broker required — we simulate broker responses inline.

import asyncio
import base64
import json

import aiohttp
import pytest
from aiohttp import web

from streamq import AsyncClient, StreamqConflict, StreamqNotFound


def json_response(data, status=200):
    """Build a broker-style JSON envelope response."""
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps({"status": "success", "data": data}),
    )


def error_response(message, status):
    """Build a broker-style error response."""
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps({"status": "error", "message": message}),
    )


@pytest.fixture
async def mock_app():
    """Returns a factory for creating aiohttp test apps with custom routes."""

    def make_app(routes):
        app = web.Application()
        for method, path, handler in routes:
            app.router.add_route(method, path, handler)
        return app

    return make_app


@pytest.fixture
async def client_for(mock_app, aiohttp_client):
    """Returns a factory that creates an AsyncClient pointed at a mock app."""

    async def factory(routes):
        app = mock_app(routes)
        server = await aiohttp_client(app)
        base_url = str(server.make_url(""))
        client = AsyncClient(base_url.rstrip("/"))
        await client.open()
        return client, server

    return factory


@pytest.mark.asyncio
async def test_create_topic_success(client_for):
    async def handler(request):
        body = await request.json()
        assert body["name"] == "payments"
        return json_response({"name": "payments"}, status=201)

    client, _ = await client_for([("POST", "/topics", handler)])
    await client.create_topic("payments")
    await client.close()


@pytest.mark.asyncio
async def test_create_topic_conflict(client_for):
    async def handler(request):
        return error_response("topic already exists", 409)

    client, _ = await client_for([("POST", "/topics", handler)])
    with pytest.raises(StreamqConflict):
        await client.create_topic("payments")
    await client.close()


@pytest.mark.asyncio
async def test_delete_topic_success(client_for):
    async def handler(request):
        return json_response(None)

    client, _ = await client_for([("DELETE", "/topics/payments", handler)])
    await client.delete_topic("payments")
    await client.close()


@pytest.mark.asyncio
async def test_delete_topic_not_found(client_for):
    async def handler(request):
        return error_response("topic not found", 404)

    client, _ = await client_for([("DELETE", "/topics/missing", handler)])
    with pytest.raises(StreamqNotFound):
        await client.delete_topic("missing")
    await client.close()


@pytest.mark.asyncio
async def test_list_topics_returns_parsed_list(client_for):
    async def handler(request):
        return json_response(
            [
                {
                    "name": "payments",
                    "message_count": 10,
                    "subscriber_count": 2,
                    "latest_offset": 9,
                },
                {
                    "name": "events",
                    "message_count": 5,
                    "subscriber_count": 0,
                    "latest_offset": 4,
                },
            ]
        )

    client, _ = await client_for([("GET", "/topics", handler)])
    topics = await client.list_topics()
    await client.close()

    assert len(topics) == 2
    assert topics[0].name == "payments"
    assert topics[0].message_count == 10
    assert topics[1].name == "events"


@pytest.mark.asyncio
async def test_list_topics_empty(client_for):
    async def handler(request):
        return json_response([])

    client, _ = await client_for([("GET", "/topics", handler)])
    topics = await client.list_topics()
    await client.close()

    assert topics == []


@pytest.mark.asyncio
async def test_get_topic_returns_info(client_for):
    async def handler(request):
        return json_response(
            {
                "name": "payments",
                "message_count": 42,
                "subscriber_count": 1,
                "latest_offset": 41,
            }
        )

    client, _ = await client_for([("GET", "/topics/payments", handler)])
    info = await client.get_topic("payments")
    await client.close()

    assert info.name == "payments"
    assert info.latest_offset == 41


@pytest.mark.asyncio
async def test_get_topic_not_found(client_for):
    async def handler(request):
        return error_response("topic not found", 404)

    client, _ = await client_for([("GET", "/topics/missing", handler)])
    with pytest.raises(StreamqNotFound):
        await client.get_topic("missing")
    await client.close()


@pytest.mark.asyncio
async def test_publish_encodes_payload_as_base64(client_for):
    received_payload = None

    async def handler(request):
        nonlocal received_payload
        body = await request.json()
        received_payload = body.get("payload")
        return json_response(
            {
                "offset": 0,
                "topic": "payments",
                "timestamp": "2025-01-01T00:00:00Z",
            },
            status=202,
        )

    client, _ = await client_for([("POST", "/topics/payments/publish", handler)])
    raw = b'{"amount": 100}'
    result = await client.publish("payments", raw)
    await client.close()

    # Verify the payload was base64-encoded in the request.
    assert received_payload == base64.b64encode(raw).decode()
    assert result.offset == 0
    assert result.topic == "payments"


@pytest.mark.asyncio
async def test_publish_not_found(client_for):
    async def handler(request):
        return error_response("topic not found", 404)

    client, _ = await client_for([("POST", "/topics/missing/publish", handler)])
    with pytest.raises(StreamqNotFound):
        await client.publish("missing", b"data")
    await client.close()


def test_consumer_builds_ws_url():
    from streamq.consumer import AsyncConsumer

    consumer = AsyncConsumer(
        "http://localhost:8080",
        "payments",
        group="billing",
        from_offset=0,
        protocol="ws",
    )
    url = consumer._build_url("ws")
    assert url.startswith("ws://localhost:8080")
    assert "/topics/payments/subscribe/ws" in url
    assert "group=billing" in url
    assert "from_offset=0" in url


def test_consumer_builds_sse_url_no_group():
    from streamq.consumer import AsyncConsumer

    consumer = AsyncConsumer(
        "http://localhost:8080",
        "events",
        from_offset=-1,  # tail — should not appear in URL
        protocol="sse",
    )
    url = consumer._build_url("sse")
    assert url.startswith("http://localhost:8080")
    assert "/topics/events/subscribe/sse" in url
    assert "group" not in url
    assert "from_offset" not in url


def test_consumer_builds_https_to_wss():
    from streamq.consumer import AsyncConsumer

    consumer = AsyncConsumer(
        "https://broker.example.com",
        "events",
        protocol="ws",
    )
    url = consumer._build_url("ws")
    assert url.startswith("wss://")
