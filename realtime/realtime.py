import time
import uuid
from datetime import datetime, UTC
import asyncio
import threading
import nats
import nats.js.api as nats_config
import json

class Realtime:
    __event_func = {}
    __topic_map = []

    CONNECTED = "CONNECTED"
    RECONNECT = "RECONNECT"
    MESSAGE_RESEND = "MESSAGE_RESEND"
    DISCONNECTED = "DISCONNECTED"

    # Private status messages
    __RECONNECTING = "RECONNECTING"
    __RECONNECTED = "RECONNECTED"
    __RECONN_FAIL = "RECONN_FAIL"

    __natsClient = None
    __jetstream = None
    __jsManager = None
    __consumerMap = {}

    __reconnected = False
    __disconnected = True
    __reconnecting = False
    __connected = False
    __reconnected_attempt = False

    __offlineMessageBuffer = []

    def __init__(self, config=None):
        if config is not None:
            if type(config) is not dict:
                raise ValueError("Realtime($config). $config not object => {}")

            if "api_key" in config:
                self.api_key = config["api_key"]

                if type(self.api_key) is not str:
                    raise ValueError("api_key value must be a string")
                
                if self.api_key == "":
                    raise ValueError("api_key value must not be an empty string")
            else:
                raise ValueError("api_key value not found in config object")
            
            if "secret" in config:
                self.secret = config["secret"]

                if type(self.secret) is not str:
                    raise ValueError("secret value must be a string")
                
                if self.secret == "":
                    raise ValueError("secret value must not be an empty string")
            else:
                raise ValueError("secret value not found in config object")
        else:
            raise ValueError("Realtime($config). $config is None")

        self.__namespace = ""

        with open("user.creds", "w") as file:
            file.write(self.__getCreds())
            file.close()
        

    def init(self, staging=False, opts=None):
        """
        Initializes library with configuration options.
        """

        self.staging = staging
        self.opts = opts

        if opts:
            if "debug" in opts:
                self.__debug = opts["debug"]
            else:
                self.__debug = False
        else:
            self.__debug = False

        self.__base_url = [
            "nats://0.0.0.0:4221",
            "nats://0.0.0.0:4222",
            "nats://0.0.0.0:4223",
            "nats://0.0.0.0:4224",
            "nats://0.0.0.0:4225",
            "nats://0.0.0.0:4226",
        ] if staging else [
            "nats://api.relay-x.io:4221",
            "nats://api.relay-x.io:4222",
            "nats://api.relay-x.io:4223",
            "nats://api.relay-x.io:4224",
            "nats://api.relay-x.io:4225",
            "nats://api.relay-x.io:4226",
        ]

    async def __get_namespace(self):
        """
        Gets the __namespace of the user using a service
        """

        encoded = self.__encode_json({
            "api_key": self.api_key
        })

        response = None
        try:
            response = await self.__natsClient.request("accounts.user.get_namespace", encoded, timeout=5)
        except Exception as e:
            self.__log(f"Error getting namespace: {e}")
            response = None
        
        if response:
            resp_data = response.data.decode('utf-8')
            resp_data = json.loads(resp_data)
            
            if resp_data["status"] == "NAMESPACE_RETRIEVE_SUCCESS":
                self.__namespace = resp_data["data"]["namespace"]
            else:
                raise ValueError("Namespace not found")
        else:
            raise ValueError("Namespace not found")

    async def connect(self):
        async def __connect():
            options = {
                "servers": self.__base_url,
                "no_echo": True,
                "max_reconnect_attempts": 1200,
                "reconnect_time_wait": 1,
                # "reconnect": True,
                "token": self.api_key,
                "user_credentials": "user.creds",
                "reconnected_cb": self.__on_reconnect,
                "disconnected_cb": self.__on_disconnect,
                "error_cb": self.__on_error,
                "closed_cb": self.__on_closed
            }

            self.__natsClient = await nats.connect(**options)
            self.__jetstream = self.__natsClient.jetstream()
            self.__log("Connected to Relay!")

            self.__connected = True
            self.__disconnected = False

            await self.__get_namespace()

            # Call the callback function if present
            if self.CONNECTED in self.__event_func:
                if self.__event_func[self.CONNECTED]:
                    self.__event_func[self.CONNECTED]()

            await self.__subscribe_to_topics()

            if not self.__reconnecting:
                # Execute on first time connection
                self.__log("Reconnection attempt not in progress")
            else:
                self.__on_reconnect()

        self.__run_async(__connect)

    async def __on_disconnect(self):
        self.__log("Disconnected from server")
        self.__disconnected = True
        self.__consumerMap.clear()
        self.__connected = False

        if self.DISCONNECTED in self.__event_func:
            self.__event_func[self.DISCONNECTED]()

        if not self.__manual_disconnect:
            # This was not a manual disconnect.
            # Reconnection attempts will be made
            self.__on_reconnect_attempt()

    async def __on_reconnect(self):
        self.__log("Reconnected!")
        self.__reconnecting = False
        self.__connected = True

        await self.__subscribe_to_topics()

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONNECTED)

        # Publish messages issued when client was in reconnection state
        output = self.__publish_messages_on_reconnect()

        if len(output) > 0:
            if self.MESSAGE_RESEND in self.__event_func:
                self.__event_func[self.MESSAGE_RESEND](output)

    async def __on_error(self, e):
        self.__log(f"There was an error: {e}")

    async def __on_closed(self):
        self.__log("Connection is closed")

    def __on_reconnect_attempt(self):
        self.__log(f"Reconnection attempt underway...")

        self.__reconnecting = True

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONNECTING)

    def __on_reconnect_failed(self):
        self.__log("Reconnection failed")

        self.__reconnected_attempt = False

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONN_FAIL)

        self.__offline_message_buffer.clear()

    def close(self):
        """
        Closes connection to server
        """
        if self.__natsClient != None:
            self.__reconnected = False
            self.__disconnected = True

            self.__manual_disconnect = True

            self.__natsClient.close()
        else:
            self.__manual_disconnect = False

            self.__log("None / null socket object, cannot close connection")

    async def publish(self, topic, data):
        if topic == None:
            raise ValueError("$topic cannot be None.")
        
        if topic == "":
            raise ValueError("$topic cannot be an empty string.")
        
        if not isinstance(topic, str):
            raise ValueError("$topic must be a string.")
        
        if not self.__is_topic_valid(topic):
            raise ValueError("$topic is not valid, use __is_topic_valid($topic) to validate topic")

        message_id = str(uuid.uuid4())

        message = {
            "client_id": self.__get_client_id(),
            "id": message_id,
            "room": topic,
            "message": data,
            "start": int(datetime.now(UTC).timestamp())
        }

        encoded = self.__encode_json(message)

        if self.__connected:
            if topic not in self.__topic_map:
                self.__topic_map.append(topic)

                await self.__start_or_get_stream()
            else:
                self.__log(f"{topic} exitsts locally, moving on...")

            topic = self.__get_stream_topic(topic)
            self.__log(f"Publishing to topic => {self.__get_stream_topic(topic)}")
            
            ack = await self.__jetstream.publish(topic, encoded)
            self.__log(f"Publish ack => {ack}")

            return ack != None
        else:
            self.__offline_message_buffer.append({
                "topic": topic,
                "message": data
            })

            return False

    @classmethod
    def on(cls, topic):
        """
        Registers a callback function for a given topic or event.

        Args:
            topic (str): The topic or event name.
            func (callable): The callback function to execute.

        Returns:
            bool: True if successfully registered, False otherwise.
        """
        def wrapper(func):
            if topic == None:
                raise ValueError("$topic cannot be None.")
            
            if func == None:
                raise ValueError("$func cannot be None.")

            if not callable(func):
                raise ValueError("The callback must be a callable function.")
            
            if not isinstance(topic, str):
                raise ValueError("The topic must be a string.")
            
            if topic not in cls.__event_func:
                cls.__event_func[topic] = func

            __temp_topic_map = cls.__topic_map.copy()
            __temp_topic_map += [cls.CONNECTED, cls.DISCONNECTED, cls.RECONNECT, cls.__RECONNECTED, 
                                    cls.__RECONNECTING, cls.__RECONN_FAIL, cls.MESSAGE_RESEND]

            if topic not in __temp_topic_map:
                cls.__topic_map.append(topic)

            if cls.__connected:
                cls.__start_consumer(topic)
            
            return True
        
        return wrapper

    async def off(self, topic):
        """
        Unregisters a callback function for a given topic or event.

        Args:
            topic (str): The topic or event name.

        Returns:
            bool: True if successfully unregistered, False otherwise.
        """

        if topic == None:
            raise ValueError("$topic cannot be None.")
        
        if not isinstance(topic, str):
                raise ValueError("The topic must be a string.")

        if topic in self.__event_func:
            self.__event_func.pop(topic)
            self.__topic_map.remove(topic)

            return await self.__delete_consumer(topic)
        else:
            return False

    def __delete_consumer(self, topic):
        return True

    async def __subscribe_to_topics(self):
        for topic in self.__topic_map:
            await self.__start_consumer(topic)

    async def __start_consumer(self, topic):
        self.__log(f"Starting consumer for {int(datetime.now(UTC).timestamp())}")
        
        def on_message(msg):
            data = json.loads(msg.data.decode('utf-8'))
            self.__log(f"Received message => {data}")

            msg.ack()

            if topic in self.__event_func:
                self.__event_func[topic](data)

        await self.__start_or_get_stream()

        await self.__jetstream.subscribe(self.__get_stream_topic(topic), cb=on_message, config=nats_config.ConsumerConfig(
            name=self.__get_stream_topic(topic),
            filter_subject=[self.__get_stream_topic(topic), self.__get_stream_topic(topic) + "_presence"],
            replay_policy=nats_config.ReplayPolicy.INSTANT,
            deliver_policy=nats_config.DeliverPolicy.NEW,
            ack_policy=nats_config.AckPolicy.EXPLICIT
        ))
        

    async def __start_or_get_stream(self):
        stream_name = self.__get_stream_name()
        subs = self.__get_stream_topic_list()

        try:
            stream = await self.__jetstream.stream_info(name=stream_name)
        except Exception as e:
            self.__log(f"Stream not found: {e}")
            stream = None

        self.__log(f"Stream => {stream.config.subjects}")

        if stream == None:
            await self.__jetstream.add_stream(name=stream_name,
                                              subjects=subs,)

            self.__log(f"Stream Created => {stream_name}")
        else:
            # Getting unique subjects from the stream
            fSubs = stream.config.subjects + subs + self.__get_stream_topic_presence_list()
            fSubs = list(set(fSubs))

            resp = await self.__jetstream.update_stream(nats_config.StreamConfig(name=stream_name, subjects=fSubs))

            self.__log(f"{stream_name} exists locally, updating and moving on...")

    # Utility functions
    def __is_topic_valid(self, topic):
        if topic != None and isinstance(topic, str):
            return topic not in [
                self.CONNECTED,
                self.RECONNECT,
                self.MESSAGE_RESEND,
                self.DISCONNECTED,
                self.__RECONNECTED,
                self.__RECONNECTING,
                self.__RECONN_FAIL
            ]
        else:
            return False
        
    def __get_client_id(self):
        return self.__natsClient.client_id
    
    def __retry_till_success(self, func, retries, delay, *args):
        method_output = None
        success = False

        for attempt in range(1, retries + 1):
            try:
                self.sleep(delay/1000)

                result = func(*args)

                method_output = result["output"]

                success = result["success"]

                if success:
                    self.__log(f"Successfully called {func.__name__}")
                    break
            except Exception as e:
                self.__log(f"Attempt {attempt} failed: {e}")
        
        if not success:
            self.__log(f"Failed to execute {func.__name__} after {retries} attempts")
    
        return method_output

    def __publish_messages_on_reconnect(self):
        message_sent_status = []

        for message in self.__offline_message_buffer:
            start_time = datetime.now()

            max_retries = self.__get_publish_retry()
            output = self.__retry_till_success(self.__publish, max_retries, 1, message["id"], message["room"], message["data"])

            message_sent_status.append(output)

        self.__offline_message_buffer.clear()
        
        return message_sent_status

    def is_topic_valid(self, topic):
        return topic in self.__room_key_events
    
    def encode_json(self, data):
        return json.dumps(data).encode('utf-8')
    
    def __get_stream_name(self):
        if self.__namespace:
            return f"{self.__namespace}_stream"
        else:
            self.close()
            raise ValueError("$namespace is None, Cannot initialize program with None $namespace")
    
    def __get_stream_topic(self, topic):
        return f"{self.__get_stream_name()}_{topic}"
    
    def __get_stream_topic_list(self):
        topics = []

        for topic in self.__topic_map:
            topics.append(self.__get_stream_topic(topic))

        return topics
    
    def __get_stream_topic_presence_list(self):
        topics = []

        for topic in self.__topic_map:
            topics.append(self.__get_stream_topic(topic) + "_presence")

        return topics

    def __get_publish_retry(self):
        max_retries = 0

        if self.opts:
            try:
                max_retries = self.opts["max_retries"]
            except:
                max_retries = self.__max_publish_retries
        else:
            max_retries = self.__max_publish_retries

        return max_retries

    def __run_async(self, func, *args):
        thread = threading.Thread(target=func, args=args)
        thread.start()

    def __encode_json(self, data):
        return json.dumps(data).encode('utf-8')

    def __log(self, msg):
        if self.__debug:
            print(msg)  # Replace with a logging system if necessary

    def sleep(self, seconds):
        time.sleep(seconds)

    def __getCreds(self):
        return f"""
-----BEGIN NATS USER JWT-----
{self.api_key}
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
{self.secret}
------END USER NKEY SEED------

*************************************************************
        """