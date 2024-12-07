from realtime import Realtime
import asyncio
import sys

realtime = Realtime("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjoiQXJqdW4iLCJwcm9qZWN0X2lkIjoidGVzdF9wcm9qZWN0Iiwib3JnYW5pemF0aW9uIjoiYmV5b25kX3JvYml0aWNzIiwiaXNfdmFsaWQiOnRydWUsImlhdCI6MTczMDczNTUzOX0.AMkp492uQC0BgPjcA3cy8FId9gGw8nHyZHDK5o3MyMk")
realtime.init(staging=False, opts={
    "debug": True
})

realtime.set_user({
    "user": "Python User",
    "id": 567
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

realtime.connect()

text = ""

while text != "exit":
    text = input("Enter message: ")

    if text == "exit":
        sys.exit(0)
    elif text == "off":
        realtime.off("hello")
    else:
        ack = realtime.publish("hello", {
            "message": text
        })

        print(f"PUBLISH RESPONSE => {ack}")

