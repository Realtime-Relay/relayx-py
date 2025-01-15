import pytest
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime import Realtime

class TestRealTime:
    def test_constructor(self):
        with pytest.raises(ValueError):
            rt = Realtime()
        
        with pytest.raises(ValueError):
            rt = Realtime("")
        
        with pytest.raises(ValueError):
            rt = Realtime(1234)

        with pytest.raises(ValueError):
            rt = Realtime(123.4)

        with pytest.raises(ValueError):
            rt = Realtime({})

        with pytest.raises(ValueError):
            rt = Realtime({
                "api_key": "1234"
            })
        
        with pytest.raises(ValueError):
            rt = Realtime({
                "secret": "1234"
            })

        with pytest.raises(ValueError):
            rt = Realtime({
                "secret": 1234
            })

        with pytest.raises(ValueError):
            rt = Realtime({
                "api_key": "<KEY>",
                "secret": ""
            })

        with pytest.raises(ValueError):
            rt = Realtime({
                "api_key": "",
                "secret": "<KEY>"
            })
        
        rt = Realtime({
                "api_key": "<KEY>",
                "secret": "<KEY>"
            })

    def test_init(self):
        rt = Realtime({
            "api_key": "<KEY>",
            "secret": "<KEY>"
        })

        rt.init(staging=False, opts={})

        assert rt.staging == False
        assert rt.opts == {}

        rt.init(staging=True, opts={
            "debug": True
        })

        assert rt.staging == True
        assert rt.opts == {
            "debug": True
        }
    
    @pytest.mark.asyncio
    async def test_publish_offline(self):
        self.realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })
        self.realtime.init(staging=True, opts={
            "debug": True
        })

        res = await self.realtime.publish("hello", [
                "Hello World!", "Sup!"
        ])
        
        assert res == False

    @pytest.mark.asyncio
    async def test_on(self):
        realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })

        realtime.init(staging=True, opts={
            "debug": True
        })

        async def generic_handler(data):
            print(f"[IMPL] => Generic Handler {json.dumps(data, indent=4)}")

        with pytest.raises(ValueError):
            await realtime.on(None, generic_handler)

        with pytest.raises(ValueError):
            await realtime.on(123, generic_handler)

        with pytest.raises(ValueError):
            await realtime.on(b'123', generic_handler)

        with pytest.raises(ValueError):
            await realtime.on({
                "hello": "world"
            }, generic_handler)

        with pytest.raises(ValueError):
            await realtime.on("hello.*", generic_handler)

        with pytest.raises(ValueError):
            await realtime.on("hello world", generic_handler)

        with pytest.raises(ValueError):
            await realtime.on("hello", None)
        
        with pytest.raises(ValueError):
            await realtime.on("hello", "None")
        
        with pytest.raises(ValueError):
            await realtime.on("hello", 12345)

        with pytest.raises(ValueError):
            await realtime.on("hello", b'12345')

        res = await realtime.on("hello", generic_handler)
        assert res == True

        # Realtime already has a reference of this topic, so the return val will be false
        res = await realtime.on("hello", generic_handler)
        assert res == False

    @pytest.mark.asyncio
    async def test_off(self):
        realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })

        realtime.init(staging=True, opts={
            "debug": True
        })

        with pytest.raises(ValueError):
            await realtime.off(None)

        with pytest.raises(ValueError):
            await realtime.off("")

        with pytest.raises(ValueError):
            await realtime.off(1244)

        with pytest.raises(ValueError):
            await realtime.off(b'hey')
        
        assert await realtime.off("hello") == True
        assert await realtime.off("hello") == False
        assert await realtime.off("hello") == False
        assert await realtime.off("hello") == False

    @pytest.mark.asyncio
    async def test_publish_online(self):
        realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })

        realtime.init(staging=True, opts={
            "debug": True
        })

        async def generic_handler(data):
            print(f"[IMPL] => Generic Handler {json.dumps(data, indent=4)}")

        async def onConnect():
            print("Connected!")

            with pytest.raises(ValueError):
                await realtime.publish(None, generic_handler)
            
            with pytest.raises(ValueError):
                await realtime.publish(1233, generic_handler)

            with pytest.raises(ValueError):
                await realtime.publish(generic_handler, generic_handler)

            with pytest.raises(ValueError):
                await realtime.publish("hello", None)

            with pytest.raises(ValueError):
                await realtime.publish("hello", b'hello')

            with pytest.raises(ValueError):
                await realtime.publish("hello world", b'hello')

            with pytest.raises(ValueError):
                await realtime.publish("hello*world", b'hello')

            res = await realtime.publish("hello", "generic_handler")
            assert res == True

            res = await realtime.publish("hello", 1234)
            assert res == True

            res = await realtime.publish("hello", {
                "message": "Hello World!"
            })
            assert res == True

            res = await realtime.publish("hello", [
                "Hello World!", "Sup!"
            ])
            assert res == True

            await realtime.close()

        await realtime.on(Realtime.CONNECTED, onConnect)
        await realtime.connect()
