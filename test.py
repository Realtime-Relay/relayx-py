from realtime import RealTime, channel

@RealTime.on("connect")
def connection(status):
    print(status)


@RealTime.on("disconnect")
def disconnect(status):
    print(status)

@RealTime.subscribe("test-topic")
def test_topic(status, data):
    print(status)

    if status == channel.JOIN_SUCCESS:
        real.publish("test-topic", {
            "data": "heyo!"
        })
    
    if data:
        print(data)

real = RealTime.init("hUaof9h9.cI1HYO2MxrQmrkAX2M2IvsN79Zf7hWso")
real.connect()