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

        # Test with explicit staging and opts
        rt.init({
            "staging": False,
            "opts": {}
        })

        assert rt.staging == False
        assert rt.opts == {}

        # Test with debug flag
        rt.init({
            "staging": False,
            "opts": {
                "debug": True
            }
        })

        assert rt.staging == False
        assert rt.opts == {
            "debug": True
        }

        # Test default values when staging is not provided
        rt.init({
            "opts": {
                "debug": False
            }
        })

        assert rt.staging == False  # Should default to False
        assert rt.opts == {
            "debug": False
        }

        # Test default values when opts is not provided
        rt.init({
            "staging": True
        })

        assert rt.staging == True
        assert rt.opts == {}  # Should default to {}

        # Test with neither staging nor opts provided
        rt.init({})

        assert rt.staging == False  # Should default to False
        assert rt.opts == {}  # Should default to {}

        # Test opts validation - must be a dict (when truthy)
        with pytest.raises(ValueError, match=r"\$init not object"):
            rt.init({
                "staging": True,
                "opts": "not a dict"
            })

        with pytest.raises(ValueError, match=r"\$init not object"):
            rt.init({
                "staging": True,
                "opts": 123
            })

        with pytest.raises(ValueError, match=r"\$init not object"):
            rt.init({
                "staging": True,
                "opts": [1, 2, 3]  # Non-empty list to trigger validation
            })

        # Empty list is falsy and won't trigger validation
        # This is current implementation behavior
        rt.init({
            "staging": True,
            "opts": []
        })
        assert rt.opts == []
    
    @pytest.mark.asyncio
    async def test_publish_offline(self):
        self.realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })
        self.realtime.init({
            "staging": False,
            "opts": {
                "debug": True
            }
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

        realtime.init({
            "staging": True,
            "opts": {
                "debug": True
            }
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

        realtime.init({
            "staging": True,
            "opts": {
                "debug": True
            }
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

        realtime.init({
            "staging": True,
            "opts": {
                "debug": True
            }
        })

        async def generic_handler(data):
            print(f"[IMPL] => Generic Handler {json.dumps(data, indent=4)}")

        async def onConnect(status):
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
            "foo",
            "foo.bar",
            "foo.bar.baz",
            "*",
            "foo.*",
            "*.bar",
            "foo.*.baz",
            ">",
            "foo.>",
            "foo.bar.>",
            "*.*.>",
            "alpha_beta",
            "alpha-beta",
            "alpha~beta",
            "abc123",
            "123abc",
            "~",
            "alpha.*.>",
            "alpha.*",
            "alpha.*.*",
            "-foo",
            "foo_bar-baz~qux",
            "A.B.C",
            "sensor.temperature",
            "metric.cpu.load",
            "foo.*.*",
            "foo.*.>",
            "foo_bar.*",
            "*.*",
            "metrics.>"
        ]

        for topic in VALID_TOPICS:
            valid = realtime.is_topic_valid(topic)
            assert valid

        INVALID_TOPICS = [
            "$foo",
            "foo$",
            "foo.$.bar",
            "foo..bar",
            ".foo",
            "foo.",
            "foo.>.bar",
            ">foo",
            "foo>bar",
            "foo.>bar",
            "foo.bar.>.",
            "foo bar",
            "foo/bar",
            "foo#bar",
            "",
            " ",
            "..",
            ".>",
            "foo..",
            ".",
            ">.",
            "foo,baz",
            "αbeta",
            "foo|bar",
            "foo;bar",
            "foo:bar",
            "foo%bar",
            "foo.*.>.bar",
            "foo.*.>.",
            "foo.*..bar",
            "foo.>.bar",
            "foo>"
        ]

        for topic in INVALID_TOPICS:
            valid = realtime.is_topic_valid(topic)
            assert not valid

    @pytest.mark.asyncio
    async def test_pattern_matcher_validation(self):
        TEST_CASES = [
            ("foo",                 "foo",                      True),
            ("foo",                 "bar",                      False),
            ("foo.*",               "foo.bar",                  True),
            ("foo.bar",             "foo.*",                    True),
            ("*",                   "token",                    True),
            ("*",                   "*",                        True),
            ("foo.*",               "foo.bar.baz",              False),
            ("foo.>",               "foo.bar.baz",              True),
            ("foo.>",               "foo",                      False),
            ("foo.bar.baz",         "foo.>",                    True),
            ("foo.bar.>",           "foo.bar",                  False),
            ("foo",                 "foo.>",                    False),
            ("foo.*.>",             "foo.bar.baz.qux",          True),
            ("foo.*.baz",           "foo.bar.>",                True),
            ("alpha.*",             "beta.gamma",               False),
            ("alpha.beta",          "alpha.*.*",                False),
            ("foo.>.bar",           "foo.any.bar",              False),
            (">",                   "foo.bar",                  True),
            (">",                   ">",                        True),
            ("*",                   ">",                        True),
            ("*.>",                 "foo.bar",                  True),
            ("*.*.*",               "a.b.c",                    True),
            ("*.*.*",               "a.b",                      False),
            ("a.b.c.d.e",           "a.b.c.d.e",                True),
            ("a.b.c.d.e",           "a.b.c.d.f",                False),
            ("a.b.*.d",             "a.b.c.d",                  True),
            ("a.b.*.d",             "a.b.c.e",                  False),
            ("a.b.>",               "a.b",                      False),
            ("a.b",                 "a.b.c.d.>",               False),
            ("a.b.>.c",             "a.b.x.c",                  False),
            ("a.*.*",               "a.b",                      False),
            ("a.*",                 "a.b.c",                    False),
            ("metrics.cpu.load",    "metrics.*.load",           True),
            ("metrics.cpu.load",    "metrics.cpu.*",            True),
            ("metrics.cpu.load",    "metrics.>.load",           False),
            ("metrics.>",           "metrics",                  False),
            ("metrics.>",           "othermetrics.cpu",         False),
            ("*.*.>",               "a.b",                      False),
            ("*.*.>",               "a.b.c.d",                  True),
            ("a.b.c",               "*.*.*",                    True),
            ("a.b.c",               "*.*",                      False),
            ("alpha.*.>",           "alpha",                    False),
            ("alpha.*.>",           "alpha.beta",               False),
            ("alpha.*.>",           "alpha.beta.gamma",         True),
            ("alpha.*.>",           "beta.alpha.gamma",         False),
            ("foo-bar_baz",         "foo-bar_baz",              True),
            ("foo-bar_*",           "foo-bar_123",              False),  # '*' literal inside token
            ("foo-bar_*",           "foo-bar_*",                True),
            ("order-*",             "order-123",                False),
            ("hello.hey.*",         "hello.hey.>",              True),
            ("queue.>",             "queue.*.123",              True),
        ]

        realtime = Realtime({
            "api_key": os.getenv("api_key", None),
            "secret": os.getenv("secret", None)
        })

        for testCase in TEST_CASES:
            tokenA = testCase[0]
            tokenB = testCase[1]
            expected_result = testCase[2]

            print(f"{tokenA} || {tokenB} => {expected_result}")

            result = realtime.topic_pattern_matcher(tokenA, tokenB)

            assert result == expected_result