import pytest
import pytest_asyncio
import asyncio
import json
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from relayx_py.kv_storage import KVStore


# Mock objects for JetStream
@pytest.fixture
def mock_jetstream():
    mock = Mock()

    # Mock key_value to return a KV bucket
    async def mock_key_value(*args, **kwargs):
        kv_bucket = Mock()

        # Mock get method
        async def mock_get(key):
            # Simulate key not found
            if key == "non-existent-key-xyz":
                raise Exception("Key not found")

            # Return a mock entry with value
            entry = Mock()
            if key == "test.string":
                entry.value = b"Hello World!"
            elif key == "test.number":
                entry.value = b"42"
            elif key == "test.boolean":
                entry.value = b"true"
            elif key == "test.null":
                entry.value = b"null"
            elif key == "test.array":
                entry.value = json.dumps([1, 2, 3, "four", {"five": 5}]).encode('utf-8')
            elif key == "test.object":
                entry.value = json.dumps({
                    "name": "Alice",
                    "age": 30,
                    "active": True,
                    "tags": ["user", "admin"]
                }).encode('utf-8')
            else:
                entry.value = None

            return entry

        kv_bucket.get = AsyncMock(side_effect=mock_get)

        # Mock put method
        async def mock_put(key, value):
            return Mock(seq=1)

        kv_bucket.put = AsyncMock(side_effect=mock_put)

        # Mock delete method
        async def mock_delete(key):
            return True

        kv_bucket.delete = AsyncMock(side_effect=mock_delete)

        # Mock keys method
        async def mock_keys():
            return ["test.key1", "test.key2", "test.key3"]

        kv_bucket.keys = AsyncMock(side_effect=mock_keys)

        return kv_bucket

    mock.key_value = AsyncMock(side_effect=mock_key_value)

    return mock


@pytest.fixture
def kv_store_config(mock_jetstream):
    return {
        "namespace": "test-kv-store",
        "jetstream": mock_jetstream,
        "debug": False
    }


@pytest_asyncio.fixture
async def kv_store(kv_store_config):
    store = KVStore(kv_store_config)
    await store.init()
    return store


# Constructor and Initialization Tests
class TestKVStoreConstructor:
    def test_constructor_successful_instantiation(self, mock_jetstream):
        kv = KVStore({
            "namespace": "test",
            "jetstream": mock_jetstream,
            "debug": False
        })

        assert kv is not None

    @pytest.mark.asyncio
    async def test_init_validates_namespace_null(self, mock_jetstream):
        kv = KVStore({
            "namespace": None,
            "jetstream": mock_jetstream,
            "debug": False
        })

        with pytest.raises(ValueError, match=r"\$namespace cannot be None / empty"):
            await kv.init()

    @pytest.mark.asyncio
    async def test_init_validates_empty_namespace(self, mock_jetstream):
        kv = KVStore({
            "namespace": "",
            "jetstream": mock_jetstream,
            "debug": False
        })

        with pytest.raises(ValueError, match=r"\$namespace cannot be None / empty"):
            await kv.init()

    @pytest.mark.asyncio
    async def test_init_validates_jetstream(self, mock_jetstream):
        kv = KVStore({
            "namespace": "test",
            "jetstream": None,
            "debug": False
        })

        with pytest.raises(ValueError, match=r"\$jetstream cannot be None"):
            await kv.init()


# Key Validation Tests
class TestKeyValidation:
    @pytest.mark.asyncio
    async def test_validate_key_with_null(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be null / undefined"):
            await kv_store.get(None)

    @pytest.mark.asyncio
    async def test_validate_key_with_non_string_number(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.get(123)

    @pytest.mark.asyncio
    async def test_validate_key_with_non_string_object(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.get({})

    @pytest.mark.asyncio
    async def test_validate_key_with_non_string_array(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.get([])

    @pytest.mark.asyncio
    async def test_validate_key_with_empty_string(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be empty"):
            await kv_store.get("")

    @pytest.mark.asyncio
    async def test_validate_key_with_valid_strings(self, kv_store):
        # These should not throw - we'll just verify no exception is raised
        try:
            # Access the private validation method
            kv_store._KVStore__validate_key("valid-key")
            kv_store._KVStore__validate_key("user.123")
            kv_store._KVStore__validate_key("test_key")
            kv_store._KVStore__validate_key("user/profile/data")
            kv_store._KVStore__validate_key("config=value")
            assert True
        except ValueError:
            pytest.fail("Valid keys should not raise ValueError")

    @pytest.mark.asyncio
    async def test_validate_key_with_invalid_characters_colon(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key can only contain alphanumeric characters and the following: _ - \. = /"):
            kv_store._KVStore__validate_key("user:123")

    @pytest.mark.asyncio
    async def test_validate_key_with_invalid_characters_at(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key can only contain alphanumeric characters and the following: _ - \. = /"):
            kv_store._KVStore__validate_key("user@domain")

    @pytest.mark.asyncio
    async def test_validate_key_with_invalid_characters_spaces(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key can only contain alphanumeric characters and the following: _ - \. = /"):
            kv_store._KVStore__validate_key("key with spaces")

    @pytest.mark.asyncio
    async def test_validate_key_with_invalid_characters_hash(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key can only contain alphanumeric characters and the following: _ - \. = /"):
            kv_store._KVStore__validate_key("key#hash")


# Value Validation Tests
class TestValueValidation:
    @pytest.mark.asyncio
    async def test_validate_value_with_valid_types(self, kv_store):
        # Should not throw for valid types
        try:
            kv_store._KVStore__validate_value(None)
            kv_store._KVStore__validate_value("string")
            kv_store._KVStore__validate_value(42)
            kv_store._KVStore__validate_value(3.14)
            kv_store._KVStore__validate_value(True)
            kv_store._KVStore__validate_value(False)
            kv_store._KVStore__validate_value([1, 2, 3])
            kv_store._KVStore__validate_value({"foo": "bar"})
            assert True
        except ValueError:
            pytest.fail("Valid values should not raise ValueError")


# JSON Validation Tests
class TestJSONValidation:
    @pytest.mark.asyncio
    async def test_is_json_with_various_types(self, kv_store):
        is_json = kv_store._KVStore__is_json

        assert is_json({"foo": "bar"}) is True
        assert is_json({"nested": {"obj": True}}) is True
        assert is_json([1, 2, 3]) is True
        assert is_json("string") is True
        assert is_json(123) is True
        assert is_json(None) is True


# Conversion Tests - __convert_to_bytes
class TestConvertToBytes:
    @pytest.mark.asyncio
    async def test_convert_to_bytes_null(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(None)
        assert result == b'null'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_boolean_true(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(True)
        assert result == b'true'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_boolean_false(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(False)
        assert result == b'false'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_string(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes("Hello World")
        assert result == b'Hello World'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_integer(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(42)
        assert result == b'42'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_float(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(3.14)
        assert result == b'3.14'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_array(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes([1, 2, 3])
        assert result == b'[1, 2, 3]'

    @pytest.mark.asyncio
    async def test_convert_to_bytes_object(self, kv_store):
        obj = {"name": "Alice", "age": 30}
        result = kv_store._KVStore__convert_to_bytes(obj)
        expected = json.dumps(obj).encode('utf-8')
        assert result == expected

    @pytest.mark.asyncio
    async def test_convert_to_bytes_nested_object(self, kv_store):
        obj = {
            "level1": {
                "level2": {
                    "level3": {
                        "deep": "value"
                    }
                }
            }
        }
        result = kv_store._KVStore__convert_to_bytes(obj)
        expected = json.dumps(obj).encode('utf-8')
        assert result == expected

    @pytest.mark.asyncio
    async def test_convert_to_bytes_empty_string(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes("")
        assert result == b''

    @pytest.mark.asyncio
    async def test_convert_to_bytes_zero(self, kv_store):
        result = kv_store._KVStore__convert_to_bytes(0)
        assert result == b'0'


# Conversion Tests - __convert_from_bytes
class TestConvertFromBytes:
    @pytest.mark.asyncio
    async def test_convert_from_bytes_null(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'null')
        assert result is None

    @pytest.mark.asyncio
    async def test_convert_from_bytes_true(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'true')
        assert result is True

    @pytest.mark.asyncio
    async def test_convert_from_bytes_false(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'false')
        assert result is False

    @pytest.mark.asyncio
    async def test_convert_from_bytes_string(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'Hello World')
        assert result == "Hello World"

    @pytest.mark.asyncio
    async def test_convert_from_bytes_integer(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'42')
        assert result == 42

    @pytest.mark.asyncio
    async def test_convert_from_bytes_float(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'3.14')
        assert result == 3.14

    @pytest.mark.asyncio
    async def test_convert_from_bytes_array(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'[1, 2, 3]')
        assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_convert_from_bytes_object(self, kv_store):
        data = json.dumps({"name": "Alice", "age": 30}).encode('utf-8')
        result = kv_store._KVStore__convert_from_bytes(data)
        assert result == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_convert_from_bytes_nested_object(self, kv_store):
        obj = {
            "level1": {
                "level2": {
                    "level3": {
                        "deep": "value"
                    }
                }
            }
        }
        data = json.dumps(obj).encode('utf-8')
        result = kv_store._KVStore__convert_from_bytes(data)
        assert result == obj

    @pytest.mark.asyncio
    async def test_convert_from_bytes_empty_string(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'')
        assert result == ""

    @pytest.mark.asyncio
    async def test_convert_from_bytes_zero(self, kv_store):
        result = kv_store._KVStore__convert_from_bytes(b'0')
        assert result == 0

    @pytest.mark.asyncio
    async def test_convert_from_bytes_invalid_input(self, kv_store):
        with pytest.raises(ValueError, match="Input must be bytes"):
            kv_store._KVStore__convert_from_bytes("not bytes")

    @pytest.mark.asyncio
    async def test_convert_from_bytes_non_json_string(self, kv_store):
        # Non-JSON string should be returned as-is
        result = kv_store._KVStore__convert_from_bytes(b'just a plain string')
        assert result == "just a plain string"


# Round-trip Conversion Tests
class TestRoundTripConversion:
    @pytest.mark.asyncio
    async def test_roundtrip_null(self, kv_store):
        original = None
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_boolean_true(self, kv_store):
        original = True
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_boolean_false(self, kv_store):
        original = False
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_string(self, kv_store):
        original = "Hello World"
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_integer(self, kv_store):
        original = 42
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_float(self, kv_store):
        original = 3.14
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_array(self, kv_store):
        original = [1, 2, 3, "four", {"five": 5}]
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_object(self, kv_store):
        original = {
            "name": "Alice",
            "age": 30,
            "active": True,
            "tags": ["user", "admin"]
        }
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original

    @pytest.mark.asyncio
    async def test_roundtrip_nested_object(self, kv_store):
        original = {
            "level1": {
                "level2": {
                    "level3": {
                        "deep": "value",
                        "number": 123,
                        "array": [1, 2, 3]
                    }
                }
            }
        }
        bytes_data = kv_store._KVStore__convert_to_bytes(original)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == original


# Real KV Operations Tests
class TestPutMethod:
    @pytest.mark.asyncio
    async def test_put_validates_key_null(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be null / undefined"):
            await kv_store.put(None, "value")

    @pytest.mark.asyncio
    async def test_put_validates_key_empty(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be empty"):
            await kv_store.put("", "value")

    @pytest.mark.asyncio
    async def test_put_validates_key_non_string(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.put(123, "value")

    @pytest.mark.asyncio
    async def test_put_successful(self, kv_store):
        # Should not raise an exception
        await kv_store.put("test.key", "test value")


class TestGetMethod:
    @pytest.mark.asyncio
    async def test_get_validates_key_null(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be null / undefined"):
            await kv_store.get(None)

    @pytest.mark.asyncio
    async def test_get_validates_key_empty(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be empty"):
            await kv_store.get("")

    @pytest.mark.asyncio
    async def test_get_validates_key_non_string(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.get(123)

    @pytest.mark.asyncio
    async def test_get_returns_none_for_non_existent_key(self, kv_store):
        result = await kv_store.get("non-existent-key-xyz")
        assert result is None


class TestDeleteMethod:
    @pytest.mark.asyncio
    async def test_delete_validates_key_null(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be null / undefined"):
            await kv_store.delete(None)

    @pytest.mark.asyncio
    async def test_delete_validates_key_empty(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key cannot be empty"):
            await kv_store.delete("")

    @pytest.mark.asyncio
    async def test_delete_validates_key_non_string(self, kv_store):
        with pytest.raises(ValueError, match=r"\$key must be a string"):
            await kv_store.delete(123)

    @pytest.mark.asyncio
    async def test_delete_non_existent_key_does_not_throw(self, kv_store):
        # Should not raise an exception
        await kv_store.delete("non-existent-key-to-delete")


class TestKeysMethod:
    @pytest.mark.asyncio
    async def test_keys_returns_array(self, kv_store):
        keys = await kv_store.keys()

        assert isinstance(keys, list)
        assert len(keys) >= 0


# Edge Cases
class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_edge_case_special_characters_in_key(self, kv_store):
        key = "test.special-chars_123"
        # Should not raise - valid characters
        kv_store._KVStore__validate_key(key)

    @pytest.mark.asyncio
    async def test_edge_case_zero_value(self, kv_store):
        bytes_data = kv_store._KVStore__convert_to_bytes(0)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == 0

    @pytest.mark.asyncio
    async def test_edge_case_false_value(self, kv_store):
        bytes_data = kv_store._KVStore__convert_to_bytes(False)
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result is False

    @pytest.mark.asyncio
    async def test_edge_case_empty_object(self, kv_store):
        bytes_data = kv_store._KVStore__convert_to_bytes({})
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == {}

    @pytest.mark.asyncio
    async def test_edge_case_empty_array(self, kv_store):
        bytes_data = kv_store._KVStore__convert_to_bytes([])
        result = kv_store._KVStore__convert_from_bytes(bytes_data)
        assert result == []
