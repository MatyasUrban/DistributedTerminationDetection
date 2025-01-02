import logging
import socket
import subprocess
import threading
import sys
import time
import queue
import json


# Predefined Node ID-IP Mapping
ID_IP_MAP = {
    1: "192.168.64.201",
    2: "192.168.64.202",
    3: "192.168.64.203",
    4: "192.168.64.204",
    5: "192.168.64.205",
}

# Configure Logging
log_file = "node.log"
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(levelname)s\tNode: %(node_id)s\tLogical Clock: %(clock)s\t%(message)s",
)
logger = logging.getLogger()

# Node Class
class Node:
    def __init__(self):
        self.log(logging.DEBUG, "Node initialization started.")
        self.id = None
        self.ip = self.get_local_ip()
        self.port = 5000
        self.online = False
        self.logical_clock = 0
        self.delay = 0  # Default delay for messages
        self.lock = threading.Lock()  # Lock for thread-safe operations
        self.init_node_id()
        self.log(logging.DEBUG, "Node initialization completed.")
        self.server_socket = None
        self.outgoing_queue = queue.Queue()
        self.outgoing_connections_thread = None
        self.incoming_connections_thread = None


    def get_local_ip(self):
        try:
            self.log(logging.DEBUG, "Attempting to determine local IP address.")
            result = subprocess.run(["ip", "a"], capture_output=True, text=True, check=True)
            lines = result.stdout.splitlines()

            interface_found = False
            for line in lines:
                if "enp0s1" in line:
                    interface_found = True
                if interface_found and "inet " in line:
                    ip_address = line.strip().split()[1].split('/')[0]
                    self.log(logging.DEBUG, f"Local IP address determined: {ip_address}")
                    return ip_address
            raise ValueError("enp0s1 interface not found or no IPv4 address assigned.")
        except Exception as e:
            self.log(logging.CRITICAL, f"Failed to determine local IP address: {e}")
            return "127.0.0.1"

    def init_node_id(self):
        self.log(logging.DEBUG, "Attempting to determine Node ID from IP.")
        for node_id, ip in ID_IP_MAP.items():
            if ip == self.ip:
                self.id = node_id
                break
        if self.id is None:
            self.log(logging.CRITICAL, "Failed to determine Node ID from IP.")
            sys.exit(1)
        self.log(logging.INFO, f"Initialized as Node {self.id} with IP {self.ip}")

    def log(self, level, message):
        global logger
        logger.log(
            level,
            message,
            extra={
                "node_id": getattr(self, "id", "N/A"),
                "clock": getattr(self, "logical_clock", "N/A")
            }
        )

    def enqueue_message(self, target_id, payload):
        """
        payload is a tuple: (message_type, message_content)
        We add a 'scheduled_time' to avoid stacked sleeps.
        """
        scheduled_time = time.time() + self.delay
        self.outgoing_queue.put((target_id, payload, scheduled_time))
        self.log(logging.INFO,
                 f"Enqueued message to Node {target_id} at scheduled time {scheduled_time:.2f}: {payload}")

    def accept_connections(self):
        """Accepts incoming connections and handles them."""
        self.log(logging.INFO, "Server is now accepting connections.")
        while self.online:
            try:
                conn, addr = self.server_socket.accept()
                if not self.online:  # If the server is offline, ignore the connection
                    conn.close()
                    break
                self.log(logging.INFO, f"Accepted connection from {addr}")
                threading.Thread(target=self.handle_incoming_message, args=(conn,), daemon=True).start()
            except OSError:
                self.log(logging.INFO, "Server socket has been closed; stopping connection acceptance.")
                break
            except Exception as e:
                if self.online:  # Log errors only if the server was expected to be running
                    self.log(logging.ERROR, f"Error accepting connection: {e}")
                break

    def process_outgoing_messages(self):
        """Processes outgoing messages from the queue and sends them."""
        while self.online:
            try:
                target_id, (msg_type, msg_content), scheduled_time = self.outgoing_queue.get(timeout=1)

                now = time.time()
                if now < scheduled_time:
                    time_to_wait = scheduled_time - now
                    self.log(logging.DEBUG,
                             f"Waiting {time_to_wait:.2f}s before sending message to Node {target_id}.")
                    time.sleep(time_to_wait)
                self.send_message(target_id, msg_type, msg_content)

            except queue.Empty:
                continue
            except Exception as e:
                if self.online:
                    self.log(logging.ERROR, f"Error processing outgoing message: {e}")

    def start_networking(self):
        """Initializes and starts the networking components."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.ip, self.port))  # Bind to the same port across all nodes
            self.server_socket.listen(5)  # Listen for incoming connections
            self.log(logging.INFO, f"Server socket started on {self.ip}:{self.port}")

            # Start a thread to handle incoming connections
            self.incoming_connections_thread = threading.Thread(target=self.accept_connections, daemon=True)
            self.incoming_connections_thread.start()

            # Start a thread to process outgoing messages
            self.outgoing_connections_thread = threading.Thread(target=self.process_outgoing_messages, daemon=True)
            self.outgoing_connections_thread.start()
        except Exception as e:
            self.log(logging.CRITICAL, f"Failed to start networking: {e}")

    def join(self):
        with self.lock:  # Ensure thread-safe access
            if not self.online:
                self.online = True
                self.start_networking()
                self.log(logging.INFO, "Node has joined the topology.")
            else:
                self.log(logging.WARNING, "Node is already online.")

    def stop_networking(self):
        """Stops the networking components and closes the server socket."""
        try:
            if self.server_socket:
                self.server_socket.close()  # Close the server socket to unblock accept()
                self.server_socket = None
                self.log(logging.INFO, "Server socket has been closed.")
            else:
                self.log(logging.WARNING, "Server socket is already closed.")
            if self.incoming_connections_thread and self.incoming_connections_thread.is_alive():
                self.incoming_connections_thread.join(timeout=2)
            if self.outgoing_connections_thread and self.outgoing_connections_thread.is_alive():
                self.outgoing_connections_thread.join(timeout=2)
        except Exception as e:
            self.log(logging.ERROR, f"Error stopping networking: {e}")

    def leave(self):
        with self.lock:  # Ensure thread-safe access
            if self.online:
                self.online = False
                self.stop_networking()
                self.log(logging.INFO, "Node has left the topology.")
            else:
                self.log(logging.WARNING, "Node is already offline.")

    def status(self):
        with self.lock:  # Ensure thread-safe access
            self.log(logging.DEBUG, "Status requested.")
            status_info = (
                f"Node ID: {self.id}\n"
                f"IP Address: {self.ip}\n"
                f"Online: {self.online}\n"
                f"Logical Clock: {self.logical_clock}\n"
                f"Message Delay: {self.delay}s\n"
            )
            print(status_info)
            self.log(logging.INFO, "Status displayed to user.")
    def set_delay(self, content):
        self.log(logging.DEBUG, "Delay change requested.")
        self.delay = int(content[0])
        self.log(logging.INFO, f"Delay set to: {self.delay}s.")
    def handle_cli(self):
        """Handles CLI commands in a dedicated thread."""
        self.log(logging.DEBUG, "CLI thread started.")
        print("Node CLI is ready. Type your command.")
        while True:
            try:
                full_command = input("Enter command: ").strip().lower().split()
                command = full_command[0]
                content = None
                if len(full_command)>1:
                    content = full_command[1:]
                self.log(logging.DEBUG, f"Received CLI command: {command}")
                if command == "join":
                    self.join()
                elif command == "leave":
                    self.leave()
                elif command == "status":
                    self.status()
                elif command == "delay":
                    self.set_delay(content)
                elif command == "send":
                    target_id = int(content[0])
                    message_text = " ".join(content[1:])  # capture full text
                    node.enqueue_message(target_id, ("TEXT", message_text))
                elif command == "quit":
                    self.log(logging.INFO, "Node is shutting down via CLI.")
                    sys.exit(0)
                else:
                    print("Unknown command.")
                    self.log(logging.WARNING, f"Unknown CLI command received: {command}")
            except KeyboardInterrupt:
                self.log(logging.INFO, "Node is shutting down due to KeyboardInterrupt.")
                print("\nShutting down node.")
                sys.exit(0)
            except EOFError:
                self.log(logging.INFO, "CLI input ended unexpectedly (EOFError).")
                print("\nCLI input ended. Exiting.")
                sys.exit(0)
            except Exception as e:
                self.log(logging.ERROR, f"Unhandled exception in CLI: {e}")
                print(f"Error: {e}")
                sys.exit(1)

    def send_message(self, target_id, message_type, message_content=None, replying_to=None):
        """
        Sends a JSON-structured message to a target node, updating local lamport clock.
        """
        target_ip = ID_IP_MAP.get(target_id)
        target_port = 5000
        if not target_ip:
            self.log(logging.ERROR, f"Invalid target node ID: {target_id}")
            return

        # Build the JSON payload
        json_payload = self.build_message(message_type, message_content, replying_to)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((target_ip, target_port))
                client_socket.sendall(json_payload.encode("utf-8"))
                self.log(logging.INFO, f"Sent message to Node {target_id}: {json_payload}")
        except Exception as e:
            self.log(logging.ERROR, f"Failed to send message to Node {target_id}: {e}")

    def build_message(self, message_type, message_content=None, replying_to=None):
        """
        Builds a JSON message with structured fields:
        {
          "sender_id": <number>,
          "sender_clock": <number>,
          "message_id": "senderId-senderClock" or a UUID,
          "message_type": <string>,
          "message_content": <string or null>,
          "replying_to": <original_message_id or null>
        }
        """
        # Increment logical clock before sending
        with self.lock:
            self.logical_clock += 1
            sender_clock = self.logical_clock
            sender_id = self.id

        message_id = f"{sender_id}-{sender_clock}"  # Or use: str(uuid.uuid4()) for uniqueness
        payload = {
            "sender_id": sender_id,
            "sender_clock": sender_clock,
            "message_id": message_id,
            "message_type": message_type,
            "message_content": message_content,
            "replying_to": replying_to
        }
        return json.dumps(payload)
    def parse_message(self, raw_data):
        """
        Parses the raw JSON data into a Python dict and updates local logical clock.
        Returns the parsed dictionary or None if invalid.
        """
        try:
            payload = json.loads(raw_data)
            # Extract sender clock
            sender_clock = payload.get("sender_clock", 0)
            # Update local logical clock: max(local, sender) + 1
            with self.lock:
                self.logical_clock = max(self.logical_clock, sender_clock) + 1
            return payload
        except json.JSONDecodeError:
            self.log(logging.WARNING, f"Failed to decode JSON: {raw_data}")
            return None
        except Exception as e:
            self.log(logging.ERROR, f"Error parsing message: {e}")
            return None

    def handle_incoming_message(self, conn):
        """Processes incoming messages from a connection."""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                raw_message = data.decode("utf-8")
                payload = self.parse_message(raw_message)
                if payload:
                    sender_id = payload.get("sender_id")
                    message_id = payload.get("message_id")
                    message_type = payload.get("message_type")
                    message_content = payload.get("message_content")
                    self.log(logging.INFO, f"Received {message_type} message from Node {sender_id}: {message_content} (Message ID: {message_id})")
                else:
                    self.log(logging.WARNING, f"Received invalid or unparseable message: {raw_message}")
        except Exception as e:
            self.log(logging.ERROR, f"Error handling incoming message: {e}")
        finally:
            conn.close()
            self.log(logging.INFO, "Connection closed.")

    def start(self):
        self.handle_cli()


# Main Function
if __name__ == "__main__":
    node = Node()
    node.start()
