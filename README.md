# Relay Python Library
![License](https://img.shields.io/badge/Apache_2.0-green?label=License)<br>
A powerful library for integrating real-time communication into your software stack, powered by the Relay Network.

## Features
1. Real-time communication made easy—connect, publish, and subscribe with minimal effort.
2. Automatic reconnection built-in, with a 2-minute retry window for network disruptions.
3. Message persistence during reconnection ensures no data loss when the client reconnects.

## Installation
Install the relay library by running the command below in your terminal<br>
`pip install relayx-py`

## Usage
### Prerequisites
1. Obtain API key and Secret key
2. Initialize the library
    ```python
    from relayx_py import Realtime
    import os

    realtime = Realtime({
        "api_key": os.getenv("api_key", None),
        "secret": os.getenv("secret", None)
    })
    realtime.init()

    # Initialization of topic listeners go here... (look at examples/local.py for full implementation)

    await realtime.connect()

    # Other application logic...
    ```

### Usage
1. <b>Publish</b><br>
Send a message to a topic:<br>
    ```python
    sent = await realtime.publish("power_telemetry", {
        "voltage_V": 5,
        "current_mA": 400,
        "power_W": 2 
    })

    if sent:
        print("Message was successfully sent to topic => power_telemetry")
    else:
        print("Message was not sent to topic => power_telemetry")
    ```
2. <b>Listen</b><br>
Subscribe to a topic to receive messages:<br>
    ```python
    def callback_fn(data):
        print(data)
    
    await realtime.on("power_telemetry", callback_fn)
    ```
    <b>Callback functions can be async or sync methods</b>
3. <b>Turn Off Listener</b><br>
Unsubscribe from a topic:<br>
    ```python
    unsubscribed = await realtime.off("power_telemetry")

    if unsubscribed:
        print("Successfully unsubscribed from power_telemetry")
    else:
        print("Unable to unsubscribe from power_telemetry")
    ```
4. <b>History</b><br>
Get previously published messages between a start date and end date. Dates are in UTC.
    ```python
    now = datetime.now(UTC)

    start = now - timedelta(days=4)

    end = now - timedelta(days=2)

    history = await realtime.history("hello", start, end)
    ```
    The end date is optional. Supplying only the start time will fetch all messages from the start time to now.
    ```python
    now = datetime.now(UTC)

    start = now - timedelta(days=4)

    history = await realtime.history("hello", start)
    ```
5. <b>Valid Topic Check</b><br>
Utility function to check if a particular topic is valid. <b>No spaces and * allowed</b>
    ```python
    is_valid = realtime.is_topic_valid("topic")

    print(f"Topic Valid => {isValid}")
    ```
6. <b>Sleep</b><br>
Utility async function to delay code execution
    ```python
    print("Starting code execution...")
    realtime.sleep(2) // arg is in seconds
    print("This line executed after 2 seconds")
    ```
7. <b>Close Connection to Relay</b><br>
Manually disconnect from the Relay Network
    ```python
    # Logic here

    await realtime.close()
    ```

## System Events
1. <b>CONNECTED</b><br>
This event is fired when the library connects to the Relay Network.
    ```python
    def on_connected():
        print("Connected!")

    await realtime.on(Realtime.CONNECTED, on_connected)
    ```

2. <b>RECONNECT</b><br>
This event is fired when the library reconnects to the Relay Network. This is only fired when the disconnection event is not manual, i.e, disconnection due to network issues.
    ```python
    def on_reconnect(status):
        print(f"Reconnection Status => {status}")

    await realtime.on(Realtime.RECONNECT, on_reconnect)
    ```
    `status` can have values of `RECONNECTING` & `RECONNECTED`.

    `RECONNECTING` => Reconnection attempts have begun. If `status == RECONNECTING`, the `RECONNECT` event is fired every 1 second.<br>
    `RECONNECTED` => Reconnected to the Relay Network.
3. <b>DISCONNECTED</b><br>
This event is fired when the library disconnects from the Relay Network. This includes disconnection due to network issues as well.
    ```python
    def on_disconnect():
        print("Disconnected from server")
    
    await realtime.on(Realtime.DISCONNECTED, on_disconnected)
    ```
4. <b>MESSAGE_RESEND</b><br>
This event is fired when the library resends the messages upon reconnection to the Relay Network.
    ```python
    def on_message_resend(messages):
        print("Offline messages may have been resent")
        print("Messages")
        print(messages)

    await realtime.on(Realtime.MESSAGE_RESEND, on_message_resend)
    ```
    `messages` is an array of the following object,<br>
    ```json
    {
        "topic": "<topic the message belongs to>",
        "message": "<message you sent>",
        "resent": "<boolean, indicating if the message was sent successully>"
    }
    ```

## API Reference
1. init()<br>
Initializes library with configuration options
2. connect()<br>
Connects the library to the Relay Network. This is an async function.
3. close()<br>
Disconnects the library from the Relay Network.
3. on()<br>
Subscribes to a topic. This is an async function.
     * @param {string} topic - Name of the event
     * @param {function} func - Callback function to call on user thread. Takes either async or sync function.
     * @returns {boolean} - To check if topic subscription was successful. Will return `false` if you try to subscribe to an already subscribed topic
3. off()<br>
Deletes reference to user defined event callback for a topic. This will stop listening to a topic. This is an async function.
     * @param {string} topic 
     * @returns {boolean} - To check if topic unsubscribe was successful. Will return `false` if you try to unsubscribe from an already unsubscribed topic
4. history()<br>
Get a list of messages published in the past, for a topic. This is an async function.<br>
A list of messages can be obtained using a start time and end time. End time is optional. If end time is not specified, all messages from the start time to now is returned.
     * @param {string} topic 
     * @param {datetime} start
     * @param {datetime} end
     * @returns {json Array} - List of messages published in the past
5. is_topic_valid()<br>
Checks if a topic can be used to send messages to.
     * @param {string} topic - Name of event
     * @returns {boolean} - If topic is valid or not. <b>No spaces and * allowed!</b>
6. sleep()<br>
Pauses code execution for a user defined time. Time passed into the method is in seconds.