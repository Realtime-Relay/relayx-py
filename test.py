from realtime import RealTime, channel
import asyncio

channel_init = False

@RealTime.on("connect")
def connection(status):
    print(status)


@RealTime.on("disconnect")
def disconnect(status):
    print(status)

def test_topic(status, data):
    print(status)

    if status == channel.JOIN_SUCCESS:
        channel_init = True
    
    if data:
        print(data)

real = RealTime.init("hUaof9h9.cI1HYO2MxrQmrkAX2M2IvsN79Zf7hWso")
real.debug_mode(True)
real.connect()

topic = input("Enter topic name: ")

if topic != "":
    RealTime.subscribe(topic)(test_topic)
    real.join(topic)
else:
    topic = "test-topic"

while True:
    message = input("Enter Message: ")
    
    real.publish(topic, {
        "data": message
    })