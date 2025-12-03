import tabulate
import re
from nats.js.errors import ServiceUnavailableError

class ErrorLogging:

    __auth_err_logged = False

    def __init__(self):
        self.__auth_err_logged = False

    def log_error(self, data):
        code = None
        err = data["err"]

        if type(err) == ServiceUnavailableError:
            code = err.err_code
        else:
            err = str(err)

        if code != None:
            if code == 10077:
                data = [
                    ["Event", "Message Limit Exceeded"],
                    ["Description", "Current message count for account exceeds plan defined limits. Upgrade plan to remove limits"],
                    ["Link", "https://console.relay-x.io/billing"]
                ]

                print(tabulate.tabulate(data, ["Type", "Data"], tablefmt="grid"))

                return

        # Permission violation check
        if "permissions violation" in err:
            user_op = data["op"]
            
            match = re.search(r'"([^"]*)"', err)
            topic = ""
            
            if user_op == "publish":
                if match:
                    temp_topic = match.group(1)
                    topic_parts = temp_topic.split(".")

                    topic = ""

                    for i, v in enumerate(topic_parts):
                        if i > 1:
                            if i < len(topic_parts) - 1:
                                topic += f"{v}."
                            else:
                                topic += v

                # This is a publish permissions violation!
                data = [
                    ["Event", "Publish Permissions Violation"],
                    ["Description", f"User is not permitted to publish on '{topic}'"],
                    ["Topic", topic],
                    ["Docs to Solve Issue", "<>"]
                ]

                print(tabulate.tabulate(data, ["Type", "Data"], tablefmt="grid"))
            elif user_op == "subscribe":
                if match:
                    temp_topic = match.group(1)
                    topic_parts = temp_topic.split(".")

                    topic = ""

                    for i, v in enumerate(topic_parts):
                        if i > 5:
                            if i < len(topic_parts) - 1:
                                topic += f"{v}."
                            else:
                                topic += v

                # This is a subscription permissions violation!
                data = [
                    ["Event", "Subscription Permissions Violation"],
                    ["Description", f"User is not permitted to subscribe on '{topic}'"],
                    ["Topic", topic],
                    ["Docs to Solve Issue", "<>"]
                ]

                print(tabulate.tabulate(data, ["Type", "Data"], tablefmt="grid"))
        
        if "Authorization Violation" in err and not self.__auth_err_logged:
            data = [
                ["Event", "Authentication Failure"],
                ["Description", "User failed to authenticate. Check if API key exists & if it is enabled"],
                ["Docs to Solve Issue", "<>"]
            ]

            print(tabulate.tabulate(data, ["Type", "Data"], tablefmt="grid"))

            self.__auth_err_logged = True

    def clear(self):
        self.__auth_err_logged = False
                