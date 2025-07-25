import os
import sys
from relayx_py import Realtime
import asyncio
import json
from datetime import datetime, timedelta, UTC, timezone

realtime = Realtime({
     "api_key": os.getenv("api_key", None),
    "secret": os.getenv("secret", None)
})
realtime.init(staging=False, opts={
    "debug": True
})

async def onHello(data):
    print(json.dumps(data, indent=4))

async def onConnect():
    print("[IMPL] Connected!")

    text = ""

    loop = asyncio.get_event_loop()

    while text != "exit":
        text = await loop.run_in_executor(None, input, "Enter Message: ")

        if text == "exit":
            sys.exit(0)
        elif text == "off":
            topic = await loop.run_in_executor(None, input, "Enter topic: ")
            await realtime.off(topic)
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
        else:
            topic = input("Enter topic: ")
            ack = await realtime.publish(topic, {
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
    await realtime.on("hello", onHello)
    await realtime.on("hello.*", generic_handler)
    await realtime.on("hello.>", generic_handler)
    await realtime.on("hello.hey.*", generic_handler)
    await realtime.on("hello.hey.>", generic_handler)
    await realtime.on("hello.hey.123", generic_handler)
    await realtime.on(Realtime.CONNECTED, onConnect)
    await realtime.on(Realtime.RECONNECT, on_reconnect)
    await realtime.on(Realtime.MESSAGE_RESEND, on_message_resend)

    await realtime.connect()

asyncio.run(main())

