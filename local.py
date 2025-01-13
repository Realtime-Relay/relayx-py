from realtime import Realtime
import asyncio
import sys

realtime = Realtime({
    "api_key": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJhdWQiOiJOQVRTIiwibmFtZSI6IkRyYWdvbiBQcm9ncmFtIiwic3ViIjoiVUNUSENDVjM2WjM0SzJFTUc0Q1BFSEhGTU1LUzVTS1dLUFNFWlo0REVMUU9IT0lOT1dDVVROSFAiLCJuYXRzIjp7ImRhdGEiOi0xLCJwYXlsb2FkIjotMSwic3VicyI6LTEsInB1YiI6eyJkZW55IjpbIj4iXX0sInN1YiI6eyJkZW55IjpbIj4iXX0sIm9yZ19kYXRhIjp7Im9yZ2FuaXphdGlvbiI6InN0b2tlLXNwYWNlIiwicHJvamVjdCI6IkRyYWdvbiBQcm9ncmFtIn0sImlzc3Vlcl9hY2NvdW50IjoiQUNaSUpaQ0lYU1NVVTU1WUVHTVAyMzZNSkkyQ1JJUkZGR0lENEpWUTZXUVlaVVdLTzJVN1k0QkIiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9LCJpc3MiOiJBQ1pJSlpDSVhTU1VVNTVZRUdNUDIzNk1KSTJDUklSRkZHSUQ0SlZRNldRWVpVV0tPMlU3WTRCQiIsImlhdCI6MTczNjY4OTExMSwianRpIjoiZm1CV3hCeS9XZmRKUmY5R0dKSkx0WGVDWVZCTm1jTlFSdTY5TWJOZkV1d1dyMnVXbkJ6b2tBQXQ4dzc5SkdXNnRZcjlvWCtCd1FJMUdDQVZaVUlNNVE9PSJ9.N8Na4AlrYehCoEzxAPZ8AgjdsQyyDibjr80q3yKKWZl2wJr_YYS89xwsjtqsF6hHXBjzm6rQxRQ-_7-TID1dDw",
    "secret": "SUANGKH2E2NOWYQ5MVPAUHOOE3EVZOMMIQCHGJ4LKGUHOBUNGG2LAMNTRU"
})
realtime.init(staging=True, opts={
    "debug": True
})

@Realtime.on("hello")
def onHello(data):
    print(data)

@Realtime.on(Realtime.CONNECTED)
def onConnect():
    print("[IMPL] Connected!")

@Realtime.on(Realtime.RECONNECT)
def on_reconnect(data):
    print(f"[IMPL] => onReconnect {data}")

@Realtime.on(Realtime.MESSAGE_RESEND)
def on_reconnect(data):
    print(f"[IMPL] => MESSAGE RESEND {data}")


async def main():
    text = ""
    
    await realtime.connect()

    while text != "exit":
        text = input("Enter message: ")

        if text == "exit":
            sys.exit(0)
        elif text == "off":
            realtime.off("hello")
        else:
            ack = await realtime.publish("hello", {
                "message": text
            })

            print(f"PUBLISH RESPONSE => {ack}")

asyncio.run(main())

