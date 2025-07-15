import pytest
import sys
import os
import json
from datetime import datetime, timedelta, UTC, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from relayx_py import Realtime

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

        rt.init(staging=False, opts={
            "debug": True
        })

        assert rt.staging == False
        assert rt.opts == {
            "debug": True
        }
    
    @pytest.mark.asyncio
    async def test_publish_offline(self):
        self.realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })
        self.realtime.init(staging=False, opts={
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

        realtime.init(staging=False, opts={
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

        realtime.init(staging=False, opts={
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

        realtime.init(staging=False, opts={
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
                await realtime.publish("hello world", b'hello')

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

            #######################################################################
            # History tests
            with pytest.raises(ValueError):
                await realtime.history(None)

            with pytest.raises(ValueError):
                await realtime.history(124)
            
            with pytest.raises(ValueError):
                await realtime.history({})

            with pytest.raises(ValueError):
                await realtime.history("hello", 1234)

            with pytest.raises(ValueError):
                await realtime.history(None, "1234")
            
            with pytest.raises(ValueError):
                await realtime.history(None, "")
            
            now = datetime.now(UTC)

            start = now - timedelta(days=4)
            start = start.timestamp()
            start = datetime.fromtimestamp(start, tz=timezone.utc)

            res = await realtime.history("hello", start)
            assert len(res) >= 0

            with pytest.raises(ValueError):
                res = await realtime.history("", start)

            with pytest.raises(ValueError):
                res = await realtime.history("hello", start, "None")

            with pytest.raises(ValueError):
                res = await realtime.history("hello", start, 1234)

            end = now - timedelta(days=2)
            end = start.timestamp()
            end = datetime.fromtimestamp(end, tz=timezone.utc)

            res = await realtime.history("hello", start, end)
            assert len(res) >= 0

            await realtime.close()

        await realtime.on(Realtime.CONNECTED, onConnect)
        await realtime.connect()

    @pytest.mark.asyncio
    async def test_topic_validation(self):
        realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })

        VALID_TOPICS = [
            "Orders",
            "customer_123",
            "foo-bar",
            "a,b,c",
            "*",
            "foo>*",
            "hello$world",
            "topic.123",
            "ABC_def-ghi",
            "data_stream_2025",
            "NODE*",
            "pubsub>events",
            "log,metric,error",
            "X123_Y456",
            "multi.step.topic",
            "batch-process",
            "sensor1_data",
            "finance$Q2",
            "alpha,beta,gamma",
            "Z9_Y8-X7",
            "config>*",
            "route-map",
            "STATS_2025-07",
            "msg_queue*",
            "update>patch",
            "pipeline_v2",
            "FOO$BAR$BAZ",
            "user.profile",
            "id_001-xyz",
            "event_queue>"
        ]

        for topic in VALID_TOPICS:
            valid = realtime.is_topic_valid(topic)
            assert valid

        INVALID_TOPICS = [
            "$internal",          # starts with $
            "hello world",        # space
            "topic/",             # slash
            "name?",              # ?
            "foo#bar",            # #
            "bar.baz!",           # !
            " space",             # leading space
            "tab\tchar",          # tab
            "line\nbreak",        # newline
            "comma ,",            # space+comma
            "",                   # empty string
            "bad|pipe",           # |
            "semi;colon",         # ;
            "colon:here",         # :
            "quote's",            # '
            "\"doublequote\"",    # "
            "brackets[]",         # []
            "brace{}",            # {}
            "paren()",            # ()
            "plus+sign",          # +
            "eq=val",             # =
            "gt>lt<",             # <
            "percent%",           # %
            "caret^",             # ^
            "ampersand&",         # &
            "back\\slash",        # backslash
            "中文字符",            # non‑ASCII
            "👍emoji",            # emoji
            "foo\rbar",           # carriage return
            "end "                # trailing space
        ]

        for topic in INVALID_TOPICS:
            valid = realtime.is_topic_valid(topic)
            assert not valid

        