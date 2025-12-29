import asyncio
import uuid
import json
import msgpack
import re
import inspect
from datetime import datetime, timezone
import nats.js.api as nats_config
from nats.js.errors import APIError, NoKeysError
from relayx_py.utils import ErrorLogging, Logging

class KVStore:
    __kv_store = None
    __jetstream = None

    __logger = None
    __error_logger = None

    __namespace = None

    def __init__(self, data):
        self.__namespace = data["namespace"]

        self.__jetstream = data["jetstream"]

        self.__error_logger = ErrorLogging()
        self.__logger = Logging(data["debug"])


    async def init(self):
        self.__validate_input()

        self.__kv_store = await self.__jetstream.key_value(self.__namespace)

        return self.__kv_store != None


    async def get(self, key):
        self.__validate_key(key)

        val = None

        try:
            val = await self.__kv_store.get(key)

            if val.value is not None:
                val = self.__convert_from_bytes(val.value)
            else:
                val = val.value
        except Exception as e:
            self.__logger.log(e)

        return val


    async def put(self, key, value):
        self.__validate_key(key)
        self.__validate_value(value)

        self.__logger.log(f"Creating KV pair for {key}")

        value = self.__convert_to_bytes(value)

        try:
            await self.__kv_store.put(key, value)
        except Exception as e:
             self.__error_logger.log_error({
                 "err": e
             })

    
    async def delete(self, key):
        self.__validate_key(key)

        self.__logger.log(f"Creating KV pair for {key}")

        try:
            await self.__kv_store.purge(key)
        except:
            pass

    async def keys(self):
        keys = []

        try:
            keys = await self.__kv_store.keys()
        except NoKeysError as nke:
            pass

        return keys


    def __validate_input(self):
        if self.__namespace is None or self.__namespace == "":
            raise ValueError("$namespace cannot be None / empty")
        
        if self.__jetstream is None:
            raise ValueError("$jetstream cannot be None")
 
        
    def __validate_key(self, key):
        if key is None:
            raise ValueError("$key cannot be null / undefined")

        if not isinstance(key, str):
            raise ValueError("$key must be a string")

        if key == "":
            raise ValueError("$key cannot be empty")

        # Validate key characters: only a-z, A-Z, 0-9, _, -, ., = and / are allowed
        valid_key_pattern = re.compile(r'^[a-zA-Z0-9_\-\.=\/]+$')
        if not valid_key_pattern.match(key):
            raise ValueError("$key can only contain alphanumeric characters and the following: _ - . = /")


    def __validate_value(self, value):
        value_valid = (
            value is None or
            isinstance(value, str) or
            isinstance(value, (int, float)) or
            isinstance(value, bool) or
            isinstance(value, list) or
            self.__is_json(value)
        )

        if not value_valid:
            raise ValueError(f"$value MUST be null, string, number, boolean, array or json! $value is \"{type(value).__name__}\"")


    def __is_json(self, data):
        try:
            json.dumps(str(data))
            return True
        except (TypeError, ValueError):
            return False
        
    def __convert_to_bytes(self, value):
        if value is None:
            return b'null'
        
        if isinstance(value, bool):
            # Handle bool before int (bool is subclass of int in Python)
            return b'true' if value else b'false'
        
        if isinstance(value, str):
            return value.encode('utf-8')
        
        if isinstance(value, (int, float)):
            return str(value).encode('utf-8')
        
        if isinstance(value, (list, dict)):
            # Convert arrays and JSON objects
            return json.dumps(value).encode('utf-8')
        
        # Fallback for other JSON-serializable objects
        return json.dumps(value).encode('utf-8')
    
    def __convert_from_bytes(self, data):
        if not isinstance(data, bytes):
            raise ValueError("Input must be bytes")
        
        # Decode bytes to string
        decoded_string = data.decode('utf-8')
        
        # Try to parse as JSON (handles null, true, false, numbers, arrays, objects)
        try:
            return json.loads(decoded_string)
        except json.JSONDecodeError:
            # If JSON parsing fails, return as string
            return decoded_string