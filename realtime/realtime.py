import websocket
import json
import threading
from enum import Enum

connection = Enum("connection", [
    "AUTH_SUCCESS",
    "AUTH_FAILURE",
    "READY", 
    "FAILURE",
    "DISCONNECTED",
    "MANUAL_DISCONNECT"
])

channel = Enum("channel", [
    "JOIN_SUCCESS", 
    "JOIN_FAILURE",
    "IN_CHANNEL"
])

TOPIC_INIT_INFO = "TOPIC_INIT_INFO"
TOPIC_MESSAGE = "TOPIC_MESSAGE"

class RealTime:

    # Event function map
    __event_func = {}
    __subscriber_func = {}

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.debug = False
        self.manual_disconnect = False

    @classmethod
    def init(cls, api_key=None):
        # Instantiates the class
        return cls(api_key)

    def debug_mode(self, debug):
        # Shows more debug logs
        self.debug = debug

    def __print(self, msg):
        # Prints debug only logs
        if self.debug:
            print(msg)

    def connect(self):
        # Connect to WS server on a different thread
        def run():
            # Define the WebSocket URL
            url = "ws://127.0.0.1:5000/ws/test-socket"  # Replace with your WebSocket URL

            def on_message(ws, message):
                self.__print(f"Received: {message}")

                data = json.loads(message)

                # Process messages based on "type"
                if data["type"] == TOPIC_INIT_INFO:
                    # Status of the attempt to join a topic
                    if data["status"] == "JOINED":
                        self.__dispatch_topic_message(
                            data["topic"],
                            channel.JOIN_SUCCESS,
                            None
                        )
                    else:
                        self.__dispatch_topic_message(
                            data["topic"],
                            channel.JOIN_FAILURE,
                            None
                        )
                # received message from topic
                elif data["type"] == TOPIC_MESSAGE:
                    # Send to decorated method
                    self.__dispatch_topic_message(
                        data["topic"],
                        channel.IN_CHANNEL,
                        {
                            "message": data["message"]
                        }
                    )
                

            def on_error(ws, error):
                self.__print("Error:", error)

            def on_close(ws, close_status_code, close_msg):
                self.__print("Connection closed")
                self.__print(close_msg)
                self.__print(close_status_code)

                # Send disconnect status to decorator method
                self.__dispatch_connection_event("disconnect")

            def on_open(ws):
                self.__print("Authentication successful")
                self.__print(self.__event_func)

                # Reset flag
                self.manual_disconnect = False

                # Send connect status to decorator method
                self.__dispatch_connection_event("connect")

                # Join subscribed channels
                for key in self.__subscriber_func.keys():
                    self.join(key)
            
            self.ws = websocket.WebSocketApp(url,
                                    header={
                                        "api-key": self.api_key
                                    },
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)

            # Run the WebSocket client
            self.ws.run_forever()

        threading.Thread(target=run).start()

    def publish(self, topic, message):
        self.ws.send(json.dumps({
            "type": TOPIC_MESSAGE,
            "topic": topic,
            "message": message
        }))

    def join(self, topic):
        self.ws.send(json.dumps({
                "type": "TOPIC_INIT",
                "topic": topic
            }))

    def __dispatch_connection_event(self, event):
        if event in self.__event_func:
            status = None

            if event == "connect":
                status = connection.READY
            elif event == "disconnect":
                if self.manual_disconnect:
                    status = connection.MANUAL_DISCONNECT
                else:
                    status = connection.DISCONNECTED

            # To avoid unnecessary calls
            if status:
                self.__event_func[event](status)
                

    def __dispatch_topic_message(self, event, status, data):
        if event in self.__subscriber_func:
            self.__subscriber_func[event](status, data)
    
    def close(self):
        self.manual_disconnect = True
        self.ws.close()

    @classmethod
    def __event_function_map(cls, event_name, func):
        # Maps connection event to decorated method in SDK implementation
        cls.__event_func[event_name] = func

    @classmethod
    def __subscribe_function_map(cls, topic, func):
        # Maps topic to decorated method in SDK implementation
        cls.__subscriber_func[topic] = func

    @classmethod
    def on(cls, event):
        def wrapper(func):
            # mapping event to class to call on event
            cls.__event_function_map(event, func)
            return func
            
        return wrapper
    
    @classmethod
    def subscribe(cls, topic):
        # Decorator which sends messages from topic into wrapped method
        def wrapper(func):
            # mapping event to class to call on event
            cls.__subscribe_function_map(topic, func)
            return func
            
        return wrapper