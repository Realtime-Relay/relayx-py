# RelayX Python SDK

![License](https://img.shields.io/badge/Apache_2.0-green?label=License)

Official Python SDK for integrating real-time messaging into your applications with RelayX.

---

## What is RelayX?

RelayX is a real-time messaging platform that enables instant communication between distributed systems. It provides pub/sub messaging, durable queues, and key-value storage through a simple, unified API.

---

## Installation

Install the SDK using pip:

```bash
pip install relayx_py
```

---

## Quick Start

```python
import asyncio
from relayx_py import Realtime

# Create a client
client = Realtime({
    "api_key": "your_api_key",
    "secret": "your_secret"
})

client.init({})

# Subscribe to a topic
async def on_message(data):
    print(f"Received: {data}")

async def main():
    await client.on("chat", on_message)
    await client.connect()

    # Publish a message
    await client.publish("chat", {"text": "Hello, RelayX!"})

asyncio.run(main())
```

---

## Messaging (Pub/Sub)

Publish and subscribe to topics for real-time communication.

**Publishing:**

```python
await client.publish("notifications", {"event": "user_login"})
```

**Subscribing:**

```python
async def handler(data):
    print(f"Topic: {data['topic']}, Message: {data['data']}")

await client.on("notifications", handler)
```

---

## Queues

Distribute work across multiple consumers with durable queues.

**Publishing a job:**

```python
queue = await client.init_queue("my_queue")
await queue.publish("tasks", {"task": "process_image"})
```

**Subscribing a worker:**

```python
async def worker(message):
    print(f"Processing: {message.message}")
    await message.ack()

await queue.consume({
    "topic": "tasks",
    "name": "worker_1",
    "group": "processors"
}, worker)
```

---

## Key-Value Store

Store and retrieve data with a distributed key-value store.

**Putting a key:**

```python
kv = await client.init_kv_store()
await kv.put("user:123", {"name": "Alice"})
```

**Getting a key:**

```python
value = await kv.get("user:123")
print(value)
```

---

## Documentation

For complete documentation including delivery guarantees, error handling, and advanced features, visit:

https://docs.relay-x.io

---

## License

This SDK is licensed under the Apache 2.0 License.
