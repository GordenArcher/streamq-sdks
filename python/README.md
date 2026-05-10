# streamq-python

Official Python SDK for [streamq](https://github.com/GordenArcher/streamq) — a lightweight message broker with WebSocket and SSE delivery.

## Installation

```bash
pip install streamq-python
```

Requires Python 3.9+.

---

## Quick start

### Async (FastAPI, aiohttp, async Django)

```python
import asyncio
from streamq import AsyncClient

async def main():
    async with AsyncClient("http://localhost:8080") as client:
        # Create a topic
        await client.create_topic("payments")

        # Publish a message
        result = await client.publish("payments", b'{"amount": 100, "currency": "GHS"}')
        print(f"published at offset {result.offset}")

        # Subscribe and receive messages
        async with client.subscribe("payments", group="billing", from_offset=0) as consumer:
            async for msg in consumer:
                print(f"offset {msg.offset}: {msg.payload}")
                await consumer.ack(msg.offset)  # at-least-once delivery

asyncio.run(main())
```

### Sync (Django, Flask, Celery, scripts)

```python
from streamq import SyncClient

client = SyncClient("http://localhost:8080")

client.create_topic("payments")

result = client.publish("payments", b'{"amount": 100}')
print(f"published at offset {result.offset}")

with client.subscribe("payments", group="billing", from_offset=0) as consumer:
    for msg in consumer:
        print(f"offset {msg.offset}: {msg.payload}")
        consumer.ack(msg.offset)

client.close()
```

---

## API

### `AsyncClient(base_url, **options)`

```python
from streamq import AsyncClient

async with AsyncClient(
    "http://localhost:8080",
    timeout=10.0,                  # HTTP call timeout in seconds (default: 10)
    reconnect_delay=2.0,           # seconds between reconnects (default: 2)
    max_reconnect_attempts=0,      # 0 = retry forever (default: 0)
) as client:
    ...
```

### `SyncClient(base_url, **options)`

```python
from streamq import SyncClient

client = SyncClient("http://localhost:8080",
    timeout=10.0,
    reconnect_delay=2.0,
    max_reconnect_attempts=0,
)
```

### Topic management

```python
# Create
await client.create_topic("events")

# List all topics
topics = await client.list_topics()
for t in topics:
    print(f"{t.name}: {t.message_count} messages, latest offset {t.latest_offset}")

# Get single topic stats
info = await client.get_topic("events")

# Delete
await client.delete_topic("events")
```

### Publishing

```python
# Payload is bytes — encode however you like
result = await client.publish("payments", b'{"amount": 100}')
print(f"offset: {result.offset}, timestamp: {result.timestamp}")
```

### Subscribing

```python
# Broadcast — receive every message (no group)
async with client.subscribe("payments") as consumer:
    async for msg in consumer:
        print(msg.payload)

# Consumer group — work queue (one message → one subscriber in group)
async with client.subscribe("payments", group="billing") as consumer:
    ...

# Replay from the beginning of retained history
async with client.subscribe("payments", from_offset=0) as consumer:
    ...

# Resume from a specific offset
async with client.subscribe("payments", from_offset=42) as consumer:
    ...

# SSE instead of WebSocket (no ack support, works through more proxies)
async with client.subscribe("payments", protocol="sse") as consumer:
    ...
```

### Error handling

```python
from streamq import (
    AsyncClient,
    StreamqConflict,
    StreamqNotFound,
    StreamqError,
)

try:
    await client.create_topic("payments")
except StreamqConflict:
    pass  # already exists — fine
except StreamqNotFound:
    print("not found")
except StreamqError as e:
    print(f"broker error: {e}")
```

---

## Delivery guarantees

| Setup | Guarantee | Notes |
|-------|-----------|-------|
| WebSocket + group + `ack()` | At-least-once | Broker redelivers from last committed offset on reconnect |
| WebSocket, no group | At-most-once | Auto-committed on send |
| SSE (any) | At-most-once | Unidirectional — no ack channel |

---

## Message type

```python
@dataclass
class Message:
    id: str
    offset: int
    topic: str
    payload: bytes      # base64 decoded automatically
    timestamp: datetime
```

---

## Broker

The broker is at [github.com/GordenArcher/streamq](https://github.com/GordenArcher/streamq).
