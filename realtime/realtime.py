import socketio
import requests
import time
import uuid
from datetime import datetime
import asyncio
import threading

class Realtime:
    __event_func = {}
    __topic_map = []

    CONNECTED = "CONNECTED"
    RECONNECT = "RECONNECT"
    MESSAGE_RESEND = "MESSAGE_RESEND"
    DISCONNECTED = "DISCONNECTED"

    __room_key_events = [
        "connect", "room-message", "room-join", "disconnect", "ping",
        "reconnect_attempt", "reconnect_failed", "room-message-ack",
        "exit-room", "relay-to-room", "enter-room", "set-user"
    ]

    # Private status messages
    __RECONNECTING = "RECONNECTING"
    __RECONNECTED = "RECONNECTED"
    __RECONN_FAIL = "RECONN_FAIL"

    def __init__(self, api_key):
        self.api_key = api_key
        self.__namespace = ""
        self.__base_url = ""
        self.__reconnected_attempt = False
        self.__disconnected = True
        self.__debug = False
        self.__offline_message_buffer = []
        self.__manual_disconnect = False
        self.opts = None

        self.__sio = socketio.Client(serializer='msgpack', reconnection=True, reconnection_attempts=240, reconnection_delay_max=500)

        self.__max_publish_retries = 5
        self.__timeout = 1000

        self.user = None

    def init(self, staging=False, opts=None):
        self.__debug = opts["debug"]
        self.opts = opts

        self.__base_url = "http://localhost:3000" if staging else "http://128.199.176.185:3000"
        
        self.__log(f"Base URL: {self.__base_url}")
        
        if self.api_key:
            self.__namespace = self.__get_namespace()
        else:
            raise ValueError("API key is not provided")

    def __get_namespace(self):
        """
        Gets the __namespace of the user using a REST API.

        Returns:
            bool: namespace str if successful, None otherwise.
        """
        start_time = datetime.now()
        url = f"{self.__base_url}/get-namespace"
        
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(url, headers=headers)
        
        res = response.json()

        if response.status_code == 200 and res["status"] == "SUCCESS":
            # Log response time asynchronously
            self.__run_async(self.__log_rest_response_time, start_time, "/get-namespace")

            return res["data"]["namespace"]
        return None

    def __log(self, msg):
        if self.__debug:
            print(msg)  # Replace with a logging system if necessary

    def sleep(self, seconds):
        time.sleep(seconds)

    def connect(self):
        def __connect():
            self.__sio.on("connect", handler=self.on_connect, namespace=f"/{self.__namespace}")
            self.__sio.on("room-message", handler=self.on_room_message, namespace=f"/{self.__namespace}")
            self.__sio.on("room-join", handler=self.on_room_join, namespace=f"/{self.__namespace}")
            self.__sio.on("disconnect", handler=self.on_disconnect, namespace=f"/{self.__namespace}")
            
            self.__sio.connect(self.__base_url, namespaces=f"/{self.__namespace}", auth={
                "api-key": self.api_key
            })
            self.__sio.wait()

            # This lines execute when the reconnection fails or when the the user calls close()
            self.__log("Reconnection fail or connection closed by user")

            if self.__reconnected_attempt:
                # this reconnection attempt failed
                self.__on_reconnect_failed()

        self.__run_async(__connect)

    def on_connect(self):
        self.__log(f"Connected with ID: {self.__sio.sid}")

        # Call callback function if present
        if self.CONNECTED in self.__event_func:
            if self.__event_func[self.CONNECTED]:
                self.__event_func[self.CONNECTED]()

        self.__disconnected = False

        if not self.__reconnected_attempt:
            # Execute on first time connection
            self.__retry_till_success(self.__set_remote_user, 5, 1)
            self.__subscribe_to_topics()
        else:
            self.__on_reconnect()

    def on_room_message(self, data, data2):
        self.__log(f"Received room message: {data}")

        room = data.get("room")
        if room in self.__event_func:
            self.__event_func[room]({
                "id": data.get("id"),
                "data": data.get("data")
            })

    def on_room_join(self, data, data2):
        room = data.get("room")
        event = data.get("event")

        if room in self.__event_func:
            self.__event_func[room](event)

    def on_disconnect(self):
        self.__log("Disconnected from server")
        self.__disconnected = True

        if not self.__manual_disconnect:
            # This was not a manual disconnect.
            # Reconnection attempts will be made
            self.__on_reconnect_attempt()

    def __on_reconnect(self):
        self.__log("Reconnected!")
        self.__disconnected = False
        self.__reconnected_attempt = False

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONNECTED)

        # Set remote user data again
        self.__retry_till_success(self.__set_remote_user, 5, 1)

        # Rejoin rooms
        self.__rejoin_rooms()

        # Publish messages issued when client was in reconnection state
        output = self.__publish_messages_on_reconnect()

        if len(output) > 0:
            if self.MESSAGE_RESEND in self.__event_func:
                self.__event_func[self.MESSAGE_RESEND](output)

    def __on_reconnect_attempt(self):
        self.__log(f"Reconnection attempt underway...")

        self.__reconnected_attempt = True

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONNECTING)

    def __on_reconnect_failed(self):
        self.__log("Reconnection failed")

        self.__reconnected_attempt = False

        if self.RECONNECT in self.__event_func:
            self.__event_func[self.RECONNECT](self.__RECONN_FAIL)

        self.__offline_message_buffer.clear()

    def set_user(self, user):
        self.user = user

    def get_user(self):
        return self.user if self.user else None

    def __set_remote_user(self):
        user_data = self.get_user()
        self.__log(user_data)

        if user_data:
            response = self.__emitWithAck("set-user", {"user_data": user_data})
        else:
            response = None
            self.__log("No user object found, skipping setting user")

        success = response != None and response["status"] == "USER_RECIEVED_ACK"

        return {
            "success": success,
            "output": success
        }

    def publish(self, topic, data):
        message_id = str(uuid.uuid4())

        if self.__sio.connected:
            max_retries = self.__get_publish_retry()

            return self.__retry_till_success(self.__publish, max_retries, 1, message_id, topic, data)
        else:
            self.__offline_message_buffer.append({
                "id": message_id,
                "room": topic,
                "data": data
            })

            return {
                "message": {
                    "id": message_id,
                    "topic": topic,
                    "message": data
                },
                "status": "PUBLISH_FAIL_TO_SEND",
                "sent": False,
                "connected": False
            }

    def __publish(self, message_id, topic, data):
        subscribed = False
        success = False
        err = None

        if not topic:
            return {
                "success": False,
                "output": {
                    "status": "PUBLISH_INPUT_ERR", 
                    "sent": False,
                    "connected": self.__sio.connected,
                    "message": f"topic is ${topic} || data is ${data}"
                }
            }

        process_start = datetime.now()

        if topic not in self.__topic_map:
            subscribed = self.__retry_till_success(self.__create_or_join_room, 5, 1, topic)
        else:
            subscribed = True
        
        self.__run_async(self.__log_socket_response_time, process_start, {
            "type": "topic_subscribe_only"
        })
        
        if subscribed:
            start = datetime.now()

            response = self.__emitWithAck("relay-to-room", {
                "id": message_id,
                "room": topic,
                "message": data
            })

            response["message"] = {
                "id": message_id,
                "room": topic,
                "message": data
            }
            response["sent"] = response["status"] == "ACK_SUCCESS"
            response["connected"] = self.__sio.connected

            self.__run_async(self.__log_socket_response_time, start, {
                "type": "publish_only"
            })

            success = True
        else:
            self.__log("Unable to send message, topic not subscribed to")

            response = {
                "message": {
                    "id": id,
                    "topic": topic,
                    "message": data
                },
                "status": "PUBLISH_FAIL_TO_SEND",
                "sent": False,
                "connected": self.__sio.connected,
                "message": f"Unable to subscribe to topic ${topic}"
            }

            err = response["message"]

            success = False
        
        self.__run_async(self.__log_socket_response_time, process_start, {
            "type": "publish_full", # TODO: Document
            "status": response["status"],
            "sent": response["sent"],
            "connected": response["connected"],
            "err": err
        })

        return {
            "success": success,
            "output": response
        }

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
            if not callable(func):
                raise ValueError("The callback must be a callable function.")
            
            if not isinstance(topic, str):
                raise ValueError("The topic must be a string.")
            
            if topic not in cls.__room_key_events:
                cls.__event_func[topic] = func

                __temp_topic_map = cls.__topic_map.copy()
                __temp_topic_map += [cls.CONNECTED, cls.DISCONNECTED, cls.RECONNECT, cls.__RECONNECTED, 
                                     cls.__RECONNECTING, cls.__RECONN_FAIL, cls.MESSAGE_RESEND]

                if topic not in __temp_topic_map:
                    cls.__topic_map.append(topic)
                
                return True
            
            return False
        
        return wrapper

    def off(self, topic):
        return self.__retry_till_success(self.__off, 5, 1, topic)

    def __off(self, topic):
        success = False
        response = None

        start_time = datetime.now()

        if not topic:
            return {
                "success": True,
                "output": {
                    "status": "TOPIC_EXIT",
                    "exit": False,
                    "message": f"topic is {topic}"
                }
            }

        if topic in self.__topic_map:
            response = self.__emitWithAck("exit-room", {
                "room": topic
            })
        else:
            response = {
                "status": "TOPIC_EXIT",
                "exit": True
            }
        
        self.__topic_map.remove(topic)
        success = True

        self.__run_async(self.__log_socket_response_time, start_time, {
            "type": "topic_unsubscribe", # TODO: Document
            "status": response["status"],
            "room": topic
        })

        return {
            "success": success,
            "output": response
        }

    def __create_or_join_room(self, topic):
        start_time = datetime.now()
        err = None
        subscribed = False

        try:
            if topic in self.__room_key_events:
                err = f"Reserved topic name: {topic}"
                raise ValueError(err)
            
            response = self.__emitWithAck("enter-room", {"room": topic})

            status = response["status"]
            
            if status == "JOINED_ROOM" or status == "ROOM_CREATED" or status == "ALREADY_IN_ROOM":
                subscribed = True
            else:
                subscribed = False
        except Exception as e:
            subscribed = False

            self.__log(e)

            response = {
                "status": "ROOM_JOIN_ERR",
                "err": e
            }

        self.__run_async(self.__log_socket_response_time, start_time, {
            "type": "create_or_join_room",
            "status": response["status"],
            "err": err
        })

        self.__log({
            "success": subscribed,
            "output": subscribed
        })

        return {
            "success": subscribed,
            "output": subscribed
        }

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

    def __subscribe_to_topics(self):
        for topic in self.__topic_map:
            subscribed = self.__retry_till_success(self.__create_or_join_room, 5, 1, topic)

            if not subscribed:
                self.__event_func[topic]({
                    "status": "TOPIC_SUBSCRIBE",
                    "subscribed": False
                })

    def __rejoin_rooms(self):
        for topic in self.__topic_map:
            start_time = datetime.now()

            subscribed = self.__retry_till_success(self.__create_or_join_room, 5, 1, topic)

            self.__event_func[topic]({
                "type": "RECONNECTION_STATUS",
                "initialized_topic": subscribed
            })

            self.__run_async(self.__log_socket_response_time, start_time, {
                "type": "rejoin_room", # TODO: Document
                "room": topic,
                "subscribed": subscribed
            })

    def __publish_messages_on_reconnect(self):
        message_sent_status = []

        for message in self.__offline_message_buffer:
            start_time = datetime.now()

            max_retries = self.__get_publish_retry()
            output = self.__retry_till_success(self.__publish, max_retries, 1, message["id"], message["room"], message["data"])

            self.__run_async(self.__log_socket_response_time, start_time, {
                "type": "publish_retry_on_connect"
            })

            message_sent_status.append(output)

        self.__offline_message_buffer.clear()
        
        return message_sent_status

    def is_topic_valid(self, topic):
        return topic in self.__room_key_events

    def close(self):
        """
        Closes connection to server
        """
        if self.__sio.connected:
            self.__reconnected_attempt = False
            self.__disconnected = True

            self.__manual_disconnect = True

            self.__sio.shutdown()
        else:
            self.__manual_disconnect = False

            self.__log("None / null socket object, cannot close connection")

    # Utility functions
    def __emitWithAck(self, event, data):
        ackData = None

        event_set = threading.Event()

        def ack(data):
            nonlocal ackData
            nonlocal event_set

            ackData = data
            event_set.set()
        
        self.__sio.emit(event, data, namespace=f"/{self.__namespace}", callback=ack)
        event_set.wait()

        self.__log(f"ACK_DATA => {ackData}")

        return ackData

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

    def __log_rest_response_time(self, start_time, url_part):
        end = datetime.now()
        response_time = (end - start_time).total_seconds() * 1000

        url = f"{self.__base_url}/metrics/log"
        
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.post(url, headers=headers, json={
            "url": url_part,
            "response_time": response_time
        })

        self.__log(response.json())

    def __log_socket_response_time(self, start_time, data):
        end = datetime.now()
        response_time = (end - start_time).total_seconds() * 1000

        url = f"{self.__base_url}/metrics/log"
        
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.post(url, headers=headers, json={
            "data": data,
            "response_time": response_time
        })

        self.__log(response.json())