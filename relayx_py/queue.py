import asyncio
import uuid
import json
import msgpack
import re
import inspect
from datetime import datetime, timezone
import nats.js.api as nats_config
from relayx_py.models.message import Message
from nats.js.errors import APIError

class Queue:
    # Public status constants
    CONNECTED = "CONNECTED"
    RECONNECT = "RECONNECT"
    MESSAGE_RESEND = "MESSAGE_RESEND"
    DISCONNECTED = "DISCONNECTED"
    SERVER_DISCONNECT = "SERVER_DISCONNECT"

    # Connection state
    reconnected = False
    disconnected = True
    reconnecting = False
    connected = True

    # Offline messages
    __offline_message_buffer = []

    __connect_called = False

    set_remote_user_attempts = 0
    set_remote_user_retries = 5

    def __init__(self, config):
        self.__queue_id = None
        self.__api_key = config.get("api_key")

        self.__nats_client = config.get("nats_client")
        self.__jetstream = config.get("jetstream")
        self.__jetstream_manager = None
        self.__consumer_map = {}

        self.__event_func = {}
        self.__topic_map = []

        self.__debug = config.get("debug", False)

        # Status Codes (private)
        self.__RECONNECTING = "RECONNECTING"
        self.__RECONNECTED = "RECONNECTED"
        self.__RECONN_FAIL = "RECONN_FAIL"


    async def initialize(self, queue_id):
        """Initialize the queue with a queue ID."""
        self.__queue_id = queue_id

        result = await self.__get_queue_namespace()

        # await self.__init_connection_listener()

        return result


    async def __get_queue_namespace(self):
        """Get namespace to start subscribing and publishing in the queue."""
        self.__log("Getting queue namespace data...")
        data = None

        try:
            res = await self.__nats_client.request(
                "accounts.user.get_queue_namespace",
                json.dumps({
                    "api_key": self.__api_key,
                    "queue_id": self.__queue_id
                }).encode('utf-8'),
                timeout=5
            )

            data = json.loads(res.data.decode('utf-8'))
            self.__log(data)
        except Exception as err:
            print("-------------------------")
            print("Error fetching queue namespace!")
            print(err)
            print("-------------------------")
            return False

        if data.get("status") == "NAMESPACE_RETRIEVE_SUCCESS":
            self.namespace = data.get("data", {}).get("namespace")
            self.topic_hash = data.get("data", {}).get("hash")
            return True
        else:
            self.namespace = None
            self.topic_hash = None

            code = data.get("code")

            if code == "QUEUE_NOT_FOUND":
                print("-------------------------------")
                print(f"Code: {code}")
                print("Description: The queue does not exist OR has been disabled")
                print(f"Queue ID: {self.__queue_id}")
                print("Docs Link To Resolve Problem: <>")
                print("-------------------------------")

            return False


    async def __init_connection_listener(self):
        """Connection listener to handle queue."""
        async def listen():
            async for s in self.__nats_client.status():
                status_type = s.type

                if status_type == "disconnect":
                    self.__log(f"client disconnected - {s.data}")
                    self.connected = False
                elif status_type == "reconnect":
                    self.__log("client reconnected -")
                    self.__log(s.data)

                    self.reconnecting = False
                    self.connected = True

                    # Resend any messages sent while client was offline
                    await self.__publish_messages_on_reconnect()
                elif status_type == "reconnecting":
                    self.__log("client is attempting to reconnect")

                    self.reconnecting = True
                    self.connected = False
                elif status_type == "stale_connection":
                    self.__log("client has a stale connection")

        asyncio.create_task(listen())


    async def publish(self, topic, data):
        """
        A method to send a message to a queue topic.
        Retry methods included. Stores messages in an array if offline.

        Args:
            topic (str): Name of the event
            data: Data to send

        Returns:
            bool: Success status
        """
        if topic is None:
            raise ValueError("$topic is null or undefined")

        if topic == "":
            raise ValueError("$topic cannot be an empty string")

        if not isinstance(topic, str):
            raise ValueError(f"Expected $topic type -> string. Instead received -> {type(topic)}")

        if not self.is_topic_valid(topic):
            raise ValueError("Invalid topic, use is_topic_valid($topic) to validate topic")

        if not self.is_message_valid(data):
            raise ValueError("$message must be JSON, string or number")

        start = datetime.now(timezone.utc).timestamp()
        message_id = str(uuid.uuid4())

        message = {
            "client_id": self.__get_client_id(),
            "id": message_id,
            "room": topic,
            "message": data,
            "start": int(datetime.now(timezone.utc).timestamp() * 1000)
        }

        if self.connected:
            self.__log("Encoding message via msgpack...")
            encoded_message = msgpack.packb(message)

            self.__log(f"Publishing to topic => {self.__get_stream_topic(topic)}")

            ack = None

            try:
                ack = await self.__jetstream.publish(self.__get_stream_topic(topic), encoded_message)
                self.__log("Publish Ack =>")
                self.__log(ack)

                latency = (datetime.now(timezone.utc).timestamp() * 1000) - start
                self.__log(f"Latency => {latency} ms")
            except Exception as err:
                self.__log(f"Error publishing: {err}")

            return ack is not None
        else:
            self.__offline_message_buffer.append({
                "topic": topic,
                "message": data
            })

            return False


    async def consume(self, data, func):
        """
        Subscribes to a topic.

        Args:
            data (dict): Configuration with 'topic' key
            func (callable): Callback function to call

        Returns:
            bool: True if subscription was successful
        """
        topic = data.get("topic")

        reserved_topics = [
            self.CONNECTED, self.DISCONNECTED, self.RECONNECT,
            self.__RECONNECTED, self.__RECONNECTING, self.__RECONN_FAIL,
            self.MESSAGE_RESEND, self.SERVER_DISCONNECT
        ]

        if not self.is_topic_valid(topic) and topic in reserved_topics:
            raise ValueError("Invalid Topic!")

        if not callable(func):
            raise ValueError(f"Expected $listener type -> function. Instead received -> {type(func)}")

        if not isinstance(topic, str):
            raise ValueError(f"Expected $topic type -> string. Instead received -> {type(topic)}")

        if topic in self.__event_func or topic in self.__topic_map:
            return False

        self.__event_func[topic] = func

        if topic not in reserved_topics:
            if not self.is_topic_valid(topic):
                raise ValueError("Invalid topic, use is_topic_valid($topic) to validate topic")

            self.__topic_map.append(topic)

            if self.connected:
                await self.__start_consumer(data)


    async def detach_consumer(self, topic):
        """
        Deletes reference to user defined event callback.
        This will stop listening to a topic.

        Args:
            topic (str): Topic name

        Returns:
            bool: True if unsubscribe was successful
        """
        if topic is None:
            raise ValueError("$topic is null / undefined")

        if not isinstance(topic, str):
            raise ValueError(f"Expected $topic type -> string. Instead received -> {type(topic)}")

        self.__topic_map = [item for item in self.__topic_map if item != topic]

        if topic in self.__event_func:
            del self.__event_func[topic]

        if topic in self.__consumer_map:
            del self.__consumer_map[topic]

        self.__log(f"Consumer closed => {topic}")


    async def __publish_messages_on_reconnect(self):
        """Resend messages when the client successfully reconnects."""
        message_sent_status = []

        for data in self.__offline_message_buffer:
            topic = data.get("topic")
            message = data.get("message")

            output = await self.publish(topic, message)

            message_sent_status.append({
                "topic": topic,
                "message": message,
                "resent": output
            })

        self.__offline_message_buffer.clear()

        # Send to client
        if self.MESSAGE_RESEND in self.__event_func and len(message_sent_status) > 0:
            self.__event_func[self.MESSAGE_RESEND](message_sent_status)


    async def __start_consumer(self, config):
        """
        Starts consumer for a particular topic if stream exists.

        Args:
            config (dict): Consumer configuration
        """
        self.__validate_consumer_config(config)

        async def on_message(consumer):
            while True:
                try:
                    msg = await consumer.fetch(timeout=1)
                    msg = msg[0]
                except Exception:
                    msg = None

                if not self.__check_var_ok(self.__event_func.get(topic)):
                    # consumerMap has no callback function because we called detach_consumer()
                    break

                if msg is None:
                    continue

                try:
                    self.__log("Decoding msgpack message...")
                    data = msgpack.unpackb(msg.data, raw=False)

                    msg_topic = self.__strip_stream_hash(msg.subject)

                    self.__log(data)

                    # Push topic message to main thread
                    if data.get("client_id") != self.__get_client_id():
                        topic_match = self.__topic_pattern_matcher(topic, msg_topic)

                        if topic_match:
                            message = Message({
                                "id": data.get("id"),
                                "topic": msg_topic,
                                "message": data.get("message"),
                                "msg": msg
                            })

                            await self.__event_func[topic](message)
                except Exception as err:
                    self.__log(f"Consumer err {err}")
                    await msg.nak()

        name = config.get("name")
        topic = config.get("topic")

        opts = {
            "name": name,
            "durable_name": name,
            "deliver_group": config.get("group"),
            "deliver_policy": nats_config.DeliverPolicy.NEW,
            "replay_policy": nats_config.ReplayPolicy.INSTANT,
            "filter_subject": self.__get_stream_topic(topic),
            "ack_policy": nats_config.AckPolicy.EXPLICIT,
        }

        if self.__check_var_ok(config.get("ack_wait")) and config.get("ack_wait", 0) >= 0 and isinstance(config.get("ack_wait"), (int, float)):
            opts["ack_wait"] = int(config.get("ack_wait"))  # Seconds to nanoseconds

        if self.__check_var_ok(config.get("backoff")) and isinstance(config.get("backoff"), list):
            opts["backoff"] = config.get("backoff")

        if self.__check_var_ok(config.get("max_deliver")) and config.get("max_deliver", 0) >= 0 and isinstance(config.get("max_deliver"), int):
            opts["max_deliver"] = config.get("max_deliver")
        else:
            opts["max_deliver"] = -1

        if self.__check_var_ok(config.get("max_ack_pending")) and config.get("max_ack_pending", 0) >= 0 and isinstance(config.get("max_ack_pending"), int):
            opts["max_ack_pending"] = config.get("max_ack_pending")

        consumer_info = None

        try:
            consumer_info = await self.__jetstream.consumer_info(self.__get_queue_name(), name)
        except Exception as e:
            self.__log(e)

        try:
            if consumer_info != None:
                opts.pop("deliver_policy")

            await self.__jetstream.add_consumer(stream=self.__get_queue_name(), config=nats_config.ConsumerConfig(**opts))

            self.__log(f"Consumer {"created" if consumer_info == None else "Updated"}")
        except Exception as e:
            self.__log(e)
            self.__log("Consumer create / update error")

        consumer = await self.__jetstream.pull_subscribe(self.__get_stream_topic(topic), durable= name, stream=self.__get_queue_name(), config=nats_config.ConsumerConfig(**opts))

        self.__consumer_map[topic] = consumer

        self.__execute_method(on_message, consumer)


    async def delete_consumer(self, topic):
        """Delete a consumer for a topic."""
        self.__log(topic)
        consumer = self.__consumer_map.get(topic)

        delete_result = False

        if consumer is not None:
            try:
                delete_result = await consumer.delete()
            except Exception:
                delete_result = False
        else:
            delete_result = False

        if topic in self.__consumer_map:
            del self.__consumer_map[topic]

        return delete_result


    # Utility functions
    def __get_client_id(self):
        """Get the client ID."""
        return self.__nats_client.client_id if hasattr(self.__nats_client, 'client_id') else None


    def is_topic_valid(self, topic):
        """
        Checks if a topic can be used to send messages to.

        Args:
            topic (str): Name of event

        Returns:
            bool: If topic is valid or not
        """
        if topic is not None and isinstance(topic, str):
            reserved_topics = [
                self.CONNECTED, self.RECONNECT, self.MESSAGE_RESEND,
                self.DISCONNECTED, self.__RECONNECTED, self.__RECONNECTING,
                self.__RECONN_FAIL, self.SERVER_DISCONNECT
            ]
            array_check = topic not in reserved_topics

            TOPIC_REGEX = re.compile(r"^(?!.*\$)(?:[A-Za-z0-9_*~-]+(?:\.[A-Za-z0-9_*~-]+)*(?:\.>)?|>)$")

            space_star_check = " " not in topic and bool(TOPIC_REGEX.match(topic))

            return array_check and space_star_check
        else:
            return False


    def is_message_valid(self, message):
        """Validate message format."""
        if message is None:
            raise ValueError("$message cannot be null / undefined")

        if isinstance(message, str):
            return True

        if isinstance(message, (int, float)):
            return True

        if self.__is_json(message):
            return True

        return False


    def __validate_consumer_config(self, config):
        """Validate consumer configuration."""
        if config is None:
            raise ValueError("$config (subscribe config) cannot be null / undefined")

        if not config.get("name"):
            raise ValueError("$config.name (subscribe config) cannot be null / undefined / Empty")

        if not config.get("topic"):
            raise ValueError("$config.topic (subscribe config) cannot be null / undefined")


    def __is_json(self, data):
        """Check if data is valid JSON."""
        try:
            json.dumps(str(data))
            return True
        except Exception:
            return False


    def __check_var_ok(self, variable):
        """Check if variable is not None."""
        return variable is not None


    def __get_queue_name(self):
        """Get the queue name."""
        if self.namespace is not None:
            return f"Q_{self.namespace}"
        else:
            raise ValueError("$namespace is null. Cannot initialize program with null $namespace")


    def __get_stream_topic(self, topic):
        """Get the stream topic with hash."""
        if self.topic_hash is not None:
            return f"{self.topic_hash}.{topic}"
        else:
            raise ValueError("$topicHash is null. Cannot initialize program with null $topicHash")


    def __strip_stream_hash(self, topic):
        """Remove stream hash prefix from topic."""
        return topic.replace(f"{self.topic_hash}.", "")


    def __topic_pattern_matcher(self, pattern_a, pattern_b):
        """
        Check if two NATS-style subject patterns could match the same concrete subject.

        Rules:
        - Literal tokens must be equal
        - '*' => exactly one token
        - '>' => one or more tokens and must be final token
        - '$' never allowed
        """
        a = pattern_a.split(".")
        b = pattern_b.split(".")

        i = j = 0  # cursors in a & b
        star_ai = star_aj = -1  # last '>' position in A and token count
        star_bi = star_bj = -1  # same for pattern B

        while i < len(a) or j < len(b):
            tok_a = a[i] if i < len(a) else None
            tok_b = b[j] if j < len(b) else None

            # Literal match or single-token wildcard
            single_wildcard = (tok_a == "*" and j < len(b)) or (tok_b == "*" and i < len(a))

            if (tok_a is not None and tok_a == tok_b) or single_wildcard:
                i += 1
                j += 1
                continue

            # Multi-token wildcard ">" must be final
            if tok_a == ">":
                if i != len(a) - 1 or j >= len(b):
                    return False
                star_ai = i
                i += 1
                star_aj = j + 1
                j += 1
                continue

            if tok_b == ">":
                if j != len(b) - 1 or i >= len(a):
                    return False
                star_bi = j
                j += 1
                star_bj = i + 1
                i += 1
                continue

            # Backtrack using last '>'
            if star_ai != -1:
                j = star_aj
                star_aj += 1
                continue

            if star_bi != -1:
                i = star_bj
                star_bj += 1
                continue

            # Dead end
            return False

        return True


    def __execute_method(self, handler, data):
        if inspect.iscoroutinefunction(handler):
            if data:
                asyncio.create_task(handler(data))
            else:
                asyncio.create_task(handler())
        else:
            loop = asyncio.get_running_loop()

            if data:
                loop.run_in_executor(
                    self._pool,
                    handler,
                    data
                )
            else:
                loop.run_in_executor(
                    self._pool,
                    handler
                )


    async def sleep(self, milliseconds):
        """Sleep for a duration in milliseconds."""
        await asyncio.sleep(milliseconds / 1000)


    def __log(self, msg):
        """Log message if debug is enabled."""
        if self.__debug:
            print(msg)
