import logging
import subprocess
import threading
import sys

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
    format="%(levelname)s - Node: %(node_id)s - Logical Clock: %(clock)s - %(message)s",
)
logger = logging.getLogger()

# Node Class
class Node:
    def __init__(self):
        self.log(logging.DEBUG, "Node initialization started.")
        self.id = None
        self.ip = self.get_local_ip()
        self.online = False
        self.logical_clock = 0
        self.delay = 0  # Default delay for messages
        self.lock = threading.Lock()  # Lock for thread-safe operations
        self.init_node_id()
        self.log(logging.DEBUG, "Node initialization completed.")

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

    def join(self):
        with self.lock:  # Ensure thread-safe access
            if not self.online:
                self.online = True
                self.log(logging.INFO, "Node has joined the topology.")
            else:
                self.log(logging.WARNING, "Node is already online.")

    def leave(self):
        with self.lock:  # Ensure thread-safe access
            if self.online:
                self.online = False
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

    def handle_cli(self):
        """Handles CLI commands in a dedicated thread."""
        self.log(logging.DEBUG, "CLI thread started.")
        print("Node CLI is ready. Type your command.")
        while True:
            try:
                command = input("Enter command: ").strip().lower()
                self.log(logging.DEBUG, f"Received CLI command: {command}")
                if command == "join":
                    self.join()
                elif command == "leave":
                    self.leave()
                elif command == "status":
                    self.status()
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

    def start(self):
        """Starts all threads for the node."""
        self.log(logging.DEBUG, "Starting CLI thread.")
        cli_thread = threading.Thread(target=self.handle_cli)
        cli_thread.daemon = True
        cli_thread.start()
        cli_thread.join()
        self.log(logging.DEBUG, "CLI thread has terminated.")


# Main Function
if __name__ == "__main__":
    node = Node()
    node.start()
