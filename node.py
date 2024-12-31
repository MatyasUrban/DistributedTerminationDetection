import logging
import socket
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
    format="%(levelname)s - Node: %(node_id)s - Clock: %(clock)s - %(message)s"
)
logger = logging.getLogger()

# Node Class
class Node:
    def __init__(self):
        self.id = None
        self.ip = self.get_local_ip()
        self.online = False
        self.logical_clock = 0
        self.delay = 0  # Default delay for messages
        self.init_node_id()

    def get_local_ip(self):
        hostname = socket.gethostname()
        return socket.gethostbyname(hostname)

    def init_node_id(self):
        for node_id, ip in ID_IP_MAP.items():
            if ip == self.ip:
                self.id = node_id
                break
        if self.id is None:
            logger.critical("Failed to determine Node ID from IP.", extra={"node_id": "N/A", "clock": "N/A"})
            sys.exit(1)
        logger.info(f"Initialized as Node {self.id} with IP {self.ip}", extra={"node_id": self.id, "clock": self.logical_clock})

    def log(self, level, message):
        """Logs a message with the node's logical clock."""
        global logger
        logger.log(level, message, extra={"node_id": self.id, "clock": self.logical_clock})

    def join(self):
        if not self.online:
            self.online = True
            self.log(logging.INFO, "Node has joined the topology.")
        else:
            self.log(logging.WARNING, "Node is already online.")

    def leave(self):
        if self.online:
            self.online = False
            self.log(logging.INFO, "Node has left the topology.")
        else:
            self.log(logging.WARNING, "Node is already offline.")

    def status(self):
        status_info = (
            f"Node ID: {self.id}\n"
            f"IP Address: {self.ip}\n"
            f"Online: {self.online}\n"
            f"Logical Clock: {self.logical_clock}\n"
            f"Message Delay: {self.delay}s\n"
        )
        print(status_info)
        self.log(logging.INFO, "Status requested.")

# Command-Line Interface (CLI)
def cli(node):
    print("Node CLI is ready. Type your command.")
    while True:
        try:
            command = input("Enter command: ").strip().lower()
            if command == "join":
                node.join()
            elif command == "leave":
                node.leave()
            elif command == "status":
                node.status()
            elif command == "quit":
                logger.info("Node is shutting down.", extra={"node_id": node.id, "clock": node.logical_clock})
                exit(0)
            else:
                print("Unknown command.")
        except KeyboardInterrupt:
            print("\nShutting down node.")
            exit(0)

# Main Function
def main():
    node = Node()
    cli_thread = threading.Thread(target=cli, args=(node,))
    cli_thread.daemon = True
    cli_thread.start()
    cli_thread.join()

if __name__ == "__main__":
    main()
