
class Message: 
    id = None
    message = None
    topic = None

    msg = None
    
    def __init__(self, message):
        self.id = message["id"]

        self.message = message["message"]

        self.topic = message["topic"]

        self.msg = message["msg"]

    async def ack(self):
        await self.msg.ack()


    async def nack(self, millis):
        await self.msg.nak(millis)
