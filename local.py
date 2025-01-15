from realtime import Realtime
import asyncio
import sys
import json
import os

realtime = Realtime({
    "api_key": os.getenv("api_key", None),
    "secret": os.getenv("secret", None)
})
realtime.init(staging=True, opts={
    "debug": True
})

async def onHello(data):
    print(json.dumps(data, indent=4))

def onConnect():
    print("[IMPL] Connected!")

def on_reconnect(data):
    print(f"[IMPL] => onReconnect {data}")

def on_message_resend(data):
    print(f"[IMPL] => MESSAGE RESEND {data}")

def generic_handler(data):
    print(f"[IMPL] => Generic Handler {data}")

async def main():
    text = ""

    await realtime.on("hello", onHello)
    await realtime.on(Realtime.CONNECTED, onConnect)
    await realtime.on(Realtime.RECONNECT, on_reconnect)
    await realtime.on(Realtime.MESSAGE_RESEND, on_message_resend)

    await realtime.connect()

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
        else:
            topic = input("Enter topic: ")
            ack = await realtime.publish(topic, {
                "message": text
            })

            print(f"PUBLISH RESPONSE => {ack}")

asyncio.run(main())

