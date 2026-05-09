# streamq-python

Official Python SDK for [streamq](https://github.com/GordenArcher/streamq), a lightweight message broker with WebSocket and SSE delivery.

## Installation

From PyPI:

```bash
pip install streamq-python
```

From this repository:

```bash
git clone https://github.com/GordenArcher/streamq-sdks.git
cd streamq-sdks/python
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

For local development, install the optional dev dependencies:

```bash
pip install -e ".[dev]"
```

Verify the development install:

```bash
pytest
```

## Quick Start

```python
from streamq import SyncClient

client = SyncClient("http://localhost:8080")

client.create_topic("payments")
result = client.publish("payments", b'{"amount":100,"currency":"GHS"}')

print(f"published to {result.topic} at offset {result.offset}")
```
