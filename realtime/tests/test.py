import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime import Realtime

class TestRealTime(unittest.TestCase):
    def test_constructor(self):
        with self.assertRaises(ValueError):
            rt = Realtime()
        
        with self.assertRaises(ValueError):
            rt = Realtime("")
        
        with self.assertRaises(ValueError):
            rt = Realtime(1234)

        with self.assertRaises(ValueError):
            rt = Realtime(123.4)

        with self.assertRaises(ValueError):
            rt = Realtime({})

        with self.assertRaises(ValueError):
            rt = Realtime({
                "api_key": "1234"
            })
        
        with self.assertRaises(ValueError):
            rt = Realtime({
                "secret": "1234"
            })

        with self.assertRaises(ValueError):
            rt = Realtime({
                "secret": 1234
            })

        with self.assertRaises(ValueError):
            rt = Realtime({
                "api_key": "<KEY>",
                "secret": ""
            })

        with self.assertRaises(ValueError):
            rt = Realtime({
                "api_key": "",
                "secret": "<KEY>"
            })
        
        rt = Realtime({
                "api_key": "<KEY>",
                "secret": "<KEY>"
            })

if __name__ == '__main__':
    unittest.main()