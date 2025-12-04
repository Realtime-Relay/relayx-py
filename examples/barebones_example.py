import sys
import asyncio
import os
from relayx_py import Realtime

realtime = Realtime({
    "api_key": os.getenv("api_key", None),
    "secret": os.getenv("secret", None)
})

realtime.init({
    "staging": True,
    "opts": {
        "debug": True
    }
})

# Initialization of topic listeners go here... (look at examples/example_chat.js for full implementation)
# await realtime.connect()
# Other application logic...
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

    await realtime.on("hello", generic_handler)
    await realtime.on(Realtime.CONNECTED, onConnect)
    await realtime.on(Realtime.RECONNECT, on_reconnect)
    await realtime.on(Realtime.MESSAGE_RESEND, on_message_resend)

    await realtime.connect()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())