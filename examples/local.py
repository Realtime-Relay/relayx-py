import os
import sys
from relayx_py import Realtime
import asyncio
import json
from datetime import datetime, timedelta, UTC, timezone

realtime = Realtime({
    "api_key": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJhdWQiOiJOQVRTIiwibmFtZSI6IjY5MmFkY2JkYWY1ZWQ5ZDU1ZTFiMWVjZiIsInN1YiI6IlVBM0JERzc0QVhUVEVMNlZONlhSNUpWMkNMWE8yVUFHSTVLNlNWVkJXU1NES0c2T1dIN1FRN0syIiwibmF0cyI6eyJkYXRhIjotMSwicGF5bG9hZCI6LTEsInN1YnMiOi0xLCJwdWIiOnsiZGVueSI6WyI-Il19LCJzdWIiOnsiZGVueSI6WyI-Il19LCJvcmdfZGF0YSI6eyJvcmdfaWQiOiI2OTI1ZTAzMTFiNjFkNDljZGVjMDMyNzgiLCJ2YWxpZGl0eV9rZXkiOiI4NTQ1NzY1Mi0wMWNiLTRlYWEtOGZkMi05MWRmZmE2NTJiZDciLCJyb2xlIjoidXNlciIsImFwaV9rZXlfaWQiOiI2OTJhZGNiZGFmNWVkOWQ1NWUxYjFlY2YiLCJlbnYiOiJ0ZXN0In0sImlzc3Vlcl9hY2NvdW50IjoiQUFZWENCNVZHR0tDSlRKRkZJRUY3WVMzUFZBNk1PQVRYRVRRSDdMM0hXT01TNVJTSFRQWFJCSEsiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9LCJpc3MiOiJBQVlYQ0I1VkdHS0NKVEpGRklFRjdZUzNQVkE2TU9BVFhFVFFIN0wzSFdPTVM1UlNIVFBYUkJISyIsImlhdCI6MTc2NDQxNjcwMiwianRpIjoiaU5hLzBhek9GN3hZak9aUmpYbEE5RE1jQ3JVL2FEQ0YxWmU1YlRibXk4L2pVYUpEanFBRGpTd2hGRmREcVlXS2VjeWZ4Z1VIWlhiWnJKYW1kWGRkSnc9PSJ9.PkbEkqW_rQ4iMX_BJPH6Qio3AluAo0c-VYwnobnybiCUc7PKn1bOWDuUbmIrhNr8v_fwcWmMGS2v_Na7u8SpCA",
    "secret": "SUAFR63MY4H7HB7YGHJJHE6HKCAGPAXKSSY4IGGIZHQ4A4WGXV7XSKMQ2Y"
})
realtime.init(staging=True, opts={
    "debug": True
})

async def onHello(data):
    print(json.dumps(data, indent=4))

async def queue_cb(data):
    print("Queue Data!")
    print(data.topic)
    print(data.message)

    await data.ack()

async def onConnect(status):
    print("[IMPL] Connected!")

    text = ""

    loop = asyncio.get_event_loop()

    queue = await realtime.init_queue("692adca3af5ed9d55e1b1ece")

    config = {
        "name": "Test434",
        "group": "test-group",
        "topic": "queue.>"
    }

    await queue.consume(config, queue_cb)

    config = {
        "name": "Test4341",
        "group": "test-group",
        "topic": "queue.test"
    }
    await queue.consume(config, queue_cb)

    config = {
        "name": "Test4342",
        "group": "test-group",
        "topic": "queue.*.123"
    }
    await queue.consume(config, queue_cb)

    while text != "exit":
        text = await loop.run_in_executor(None, input, "Enter Message: ")

        print(f"Message => {text}")

        if text == "exit":
            sys.exit(0)
        elif text == "off":
            topic = await loop.run_in_executor(None, input, "Enter topic: ")
            await queue.detach_consumer(topic)
        elif text == "on":
            topic = await loop.run_in_executor(None, input, "Enter topic: ")
            await realtime.on(topic, generic_handler)
        elif text == "close":
            await realtime.close()
        elif text == "init":
            await realtime.connect()
        elif text == "history":
            topic = await loop.run_in_executor(None, input, "Enter topic: ")

            now = datetime.now(UTC)

            # Subtract 2 days
            start = now - timedelta(days=4)
            start = start.timestamp()
            start = datetime.fromtimestamp(start, tz=timezone.utc)

            end = now - timedelta(days=2)
            end = start.timestamp()
            end = datetime.fromtimestamp(end, tz=timezone.utc)

            history = await realtime.history("hello", start)
            print(history)
        elif text == "delete_consumer":
            name = await loop.run_in_executor(None, input, "Enter Consumer Name: ")
            del_result = await queue.delete_consumer(name)

            print(del_result)
        elif text == "detatch":
            topic = await loop.run_in_executor(None, input, "Enter Topic: ")
            await queue.detach_consumer(topic)
        else:
            topic = input("Enter topic: ")
            ack = await queue.publish(topic, {
                "message": text
            })

            print(f"PUBLISH RESPONSE => {ack}")

def on_reconnect(data):
    print(f"[IMPL] => onReconnect {data}")

def on_message_resend(data):
    print(f"[IMPL] => MESSAGE RESEND {data}")

def generic_handler(data):
    realtime.sleep(10)
    print(f"[IMPL] => Generic Handler {data}")

async def main():
    # await realtime.on("hello", onHello)
    # await realtime.on("hello.*", generic_handler)
    # await realtime.on("hello.>", generic_handler)
    # await realtime.on("hello.hey.*", generic_handler)
    # await realtime.on("hello.hey.>", generic_handler)
    # await realtime.on("hello.hey.123", generic_handler)
    await realtime.on(Realtime.CONNECTED, onConnect)
    await realtime.on(Realtime.RECONNECT, on_reconnect)
    await realtime.on(Realtime.MESSAGE_RESEND, on_message_resend)

    await realtime.connect()

asyncio.run(main())

