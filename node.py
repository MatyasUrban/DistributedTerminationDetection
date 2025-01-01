import logging
import socket
import subprocess
import threading
import sys
import time
import queue


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

    def enqueue_message(self, target_id, message):
        """Adds a message to the outgoing queue."""
        self.outgoing_queue.put((target_id, message))
        self.log(logging.INFO, f"Enqueued message to Node {target_id}: {message}")

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
                # Socket was closed; break out of the loop
                self.log(logging.INFO, "Server socket has been closed; stopping connection acceptance.")
                break
            except Exception as e:
                if self.online:  # Log errors only if the server was expected to be running
                    self.log(logging.ERROR, f"Error accepting connection: {e}")

    def process_outgoing_messages(self):
        """Processes outgoing messages from the queue and sends them."""
        while self.online:
            try:
                target_id, message = self.outgoing_queue.get(timeout=1)  # Timeout prevents indefinite blocking
                self.log(logging.DEBUG, f"Processing message to Node {target_id}: {message}")
                if self.delay > 0:
                    self.log(logging.INFO, f"Delaying message to Node {target_id} by {self.delay} seconds.")
                    time.sleep(self.delay)
                self.send_message(target_id, message)
            except queue.Empty:
                continue
            except Exception as e:
                if self.online:  # Log errors only if the node is still online
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

    def set_status(self, status):
        """
        Sets the node's online status and starts or stops networking components accordingly.
        """
        with self.lock:  # Ensure thread-safe operations
            if status:
                if not self.online:
                    try:
                        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.server_socket.bind((self.ip, self.port))
                        self.server_socket.listen(5)
                        self.log(logging.INFO, f"Server socket started on {self.ip}:{self.port}")

                        # Start threads for networking
                        self.incoming_connections_thread = threading.Thread(
                            target=self.accept_connections, daemon=True
                        )
                        self.incoming_connections_thread.start()

                        self.outgoing_connections_thread = threading.Thread(
                            target=self.process_outgoing_messages, daemon=True
                        )
                        self.outgoing_connections_thread.start()
                        self.log(logging.INFO, "Networking components initialized.")
                        self.online = True
                    except Exception as e:
                        self.online = False  # Rollback state on failure
                        self.log(logging.CRITICAL, f"Failed to start networking: {e}")
                else:
                    self.log(logging.WARNING, "Node is already online.")
            else:
                if self.online:
                    try:
                        if self.server_socket:
                            self.server_socket.close()
                            self.server_socket = None
                            self.log(logging.INFO, "Server socket has been closed.")
                        else:
                            self.log(logging.WARNING, "Server socket was already closed.")
                        self.log(logging.INFO, "Networking components shut down.")
                        self.online = False
                    except Exception as e:
                        self.log(logging.ERROR, f"Error stopping networking: {e}")
                else:
                    self.log(logging.WARNING, "Node is already offline.")

    def join(self):
        with self.lock:
            self.set_status(True)  # Turn networking on
            self.log(logging.INFO, "Node has joined the topology.")

    def leave(self):
        with self.lock:
            self.set_status(False)  # Turn networking off
            self.log(logging.INFO, "Node has left the topology.")

    def status(self):
        with self.lock:  # Ensure thread-safe access
            self.log(logging.DEBUG, "Status requested.")
            status_info = (
                f"Node ID: {self.id}\n"
                f"IP Address: {self.ip}\n"
                f"Online: {self.online}\n"
                f"Logical Clock: {self.logical_clock}\n"
                f"Message Delay: {self.delay}ms\n"
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
                    target_id, message = content[0], content[1]
                    node.enqueue_message(int(target_id), message)
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

    def send_message(self, target_id, message):
        """Sends a message to a target node."""
        target_ip = ID_IP_MAP.get(target_id)
        target_port = 5000
        if not target_ip:
            self.log(logging.ERROR, f"Invalid target node ID: {target_id}")
            return

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((target_ip, target_port))
                client_socket.sendall(message.encode())
                self.log(logging.INFO, f"Sent message to Node {target_id}: {message}")
        except Exception as e:
            self.log(logging.ERROR, f"Failed to send message to Node {target_id}: {e}")

    def handle_incoming_message(self, conn):
        """Processes incoming messages from a connection."""
        try:
            while True:
                data = conn.recv(1024)  # Receive data (1024 bytes at a time)
                if not data:
                    break
                message = data.decode()
                self.log(logging.INFO, f"Received message: {message}")
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
