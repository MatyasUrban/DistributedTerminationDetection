import logging
import socket
import subprocess
import threading
import sys
import time
import queue
import json
import typing

class Message(typing.TypedDict):
    sender_id: int
    sender_clock: int
    message_id: str
    message_type: str
    message_content: typing.Optional[str]
    replying_to: typing.Optional[str]

# Predefined Node ID-IP Mapping
ID_IP_MAP = {
    0: "192.168.64.201",
    1: "192.168.64.202",
    2: "192.168.64.203",
    3: "192.168.64.204",
    4: "192.168.64.205",
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
        self.topology = None
        self.successor_id = None
        self.predecessor_id = None
        self.work_queue = queue.Queue()
        self.work_thread = None
        self.task_in_progress = None
        self.sent_messages = {}
        self.received_replies = {}

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

    def update_successor_and_predecessor(self):
        """
        Sets self.successor_id and self.predecessor_id based on self.topology.
        If topology has only one node (ourselves), both successor_id and predecessor_id become None.
        """
        if not self.topology or len(self.topology) == 1:
            # Single-node topology: we are alone
            self.successor_id = None
            self.predecessor_id = None
            self.log(logging.INFO, f"No successor or predecessor given that we are {'alone in the topology' if len(self.topology) == 1 else 'removed from the topology'}.")
            return

        # We assume self.id is in self.topology. We'll find our index.
        idx = self.topology.index(self.id)
        # successor is (idx+1) mod len(topology)
        succ_idx = (idx + 1) % len(self.topology)
        pred_idx = (idx - 1) % len(self.topology)

        self.successor_id = self.topology[succ_idx]
        self.predecessor_id = self.topology[pred_idx]

        self.log(logging.INFO,f"My predecessor is Node {self.predecessor_id}, my successor is Node {self.successor_id}.")

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
                target_id, message_payload, scheduled_time = self.outgoing_queue.get(timeout=1)

                now = time.time()
                if now < scheduled_time:
                    time_to_wait = scheduled_time - now
                    self.log(logging.DEBUG,
                             f"Waiting {time_to_wait:.2f}s before sending message to Node {target_id}.")
                    time.sleep(time_to_wait)
                self.send_message(target_id, message_payload)

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

    def probe_for_successor(self):
        """
        Probes potential successors in ascending ID order (skipping self.id).
        If any replies with e.g. 'JOIN_ACK', we set that node as successor.
        Otherwise, if no replies, we consider ourselves alone in the topology.
        """
        self.log(logging.INFO, "Node will now probe for successors.")
        self.topology = [self.id]  # start with only ourselves
        candidate_ids = [x for x in ID_IP_MAP.keys() if x != self.id]
        candidate_ids.sort()  # ascending order
        cand_id = (self.id + 1) % 5
        while cand_id != self.id:
            if cand_id == self.id:
                break
            self.log(logging.INFO, f"Probing successor with id {cand_id}.")
            # Send a JOIN message
            msg_id = self.build_and_enqueue_message(cand_id, "JOIN")

            # Wait up to 3 seconds or break sooner if we see a reply
            start_wait = time.time()
            while time.time() - start_wait < 3:
                if msg_id in self.received_replies:
                    reply_info = self.received_replies[msg_id]
                    self.log(logging.INFO, f"Successful probe to Node {reply_info.get('sender_id')} that is already in the topology.")
                    return  # done
                time.sleep(0.1)  # small wait, re-check
            cand_id = (cand_id + 1) % 5

        # If we reach here, no one responded
        self.log(logging.INFO, f"No successors found online. We are alone in the topology: {self.topology}")
        self.successor_id = None  # or maybe set it to self.id if you want a ring of one

    def join(self):
        if not self.online:
            self.online = True
            self.start_networking()
            self.start_work_processor()
            self.probe_for_successor()
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
                self.stop_work_processor()
                self.successor_id = None
                self.topology = None
                self.log(logging.INFO, "Node has left the topology.")
            else:
                self.log(logging.WARNING, "Node is already offline.")

    def status(self):
        with self.lock:  # Ensure thread-safe access
            self.log(logging.DEBUG, "Status requested.")
            busy_state = "BUSY" if self.is_busy() else "IDLE"
            status_info = (
                f"Node ID: {self.id}\n"
                f"IP Address: {self.ip}\n"
                f"Online: {self.online}\n"
                f"Logical Clock: {self.logical_clock}\n"
                f"Message Delay: {self.delay}s\n"
                f"Work State: {busy_state}\n"
                f"Topology: {self.topology}\n"
                f"Predecessor: {self.predecessor_id}\n"
                f"Successor: {self.successor_id}\n"
            )
            print(status_info)
            self.log(logging.INFO, "Status displayed to user.")
    def set_delay(self, content):
        self.log(logging.DEBUG, "Delay change requested.")
        self.delay = int(content[0])
        self.log(logging.INFO, f"Delay set to: {self.delay}s.")

    def process_tasks(self):
        """Continuously processes tasks from the work queue, one at a time."""
        while self.online:
            try:
                # Wait for a task to become available
                task_id, task_type, task_goal = self.work_queue.get(timeout=1)
                self.task_in_progress = (task_id, task_type)

                if task_type == "count":
                    # Perform counting from 1 to task_goal
                    for i in range(1, task_goal + 1):
                        if not self.online:
                            # If node goes offline mid-task, we stop
                            self.log(logging.INFO, "Node went offline, stopping current task.")
                            break

                        self.log(logging.INFO, f"Executing task {task_id}: count {i}/{task_goal}")
                        time.sleep(1)  # Simulate work each second

                    self.log(logging.INFO, f"Task {task_id} completed or stopped.")

                # Mark the task done
                self.task_in_progress = None
                self.work_queue.task_done()

            except queue.Empty:
                # No task in the queue, loop again
                continue
            except Exception as e:
                self.log(logging.ERROR, f"Error processing task: {e}")
                self.task_in_progress = None

    def enqueue_task(self, goal):
        """
        Enqueues a counting task.
        Increments local logical clock, then creates a unique task_id and enqueues.
        """
        with self.lock:
            self.logical_clock += 1
            current_clock = self.logical_clock

        task_id = f"{self.id}-{current_clock}-{goal}"
        self.work_queue.put((task_id, "count", goal))
        self.log(logging.INFO, f"Enqueued task: {task_id} (count up to {goal}).")

    def start_work_processor(self):
        """Starts the worker thread that processes tasks (e.g., counting)."""
        self.log(logging.DEBUG, "Starting work processor thread.")
        self.work_thread = threading.Thread(target=self.process_tasks, daemon=True)
        self.work_thread.start()
    def stop_work_processor(self):
        """Starts the worker thread that processes tasks (e.g., counting)."""
        self.log(logging.DEBUG, "Stopping work processor thread.")
        if self.work_thread and self.work_thread.is_alive():
            self.work_thread.join(timeout=2)

    def is_busy(self):
        """
        Returns True if a task is currently in progress or if the work_queue isn't empty.
        """
        in_progress = (self.task_in_progress is not None)
        queue_non_empty = not self.work_queue.empty()
        return in_progress or queue_non_empty

    def handle_cli(self):
        """Handles CLI commands in a dedicated thread."""
        self.log(logging.DEBUG, "CLI thread started.")
        print("Node CLI is ready. Type your command.")
        while True:
            try:
                full_command = input().strip().lower().split()
                command = full_command[0]
                content = None
                if len(full_command)>1:
                    content = full_command[1:]
                self.log(logging.DEBUG, f"Received CLI command: {command}")
                if command == "join":
                    self.join()
                elif command == "leave":
                    if self.topology:
                        new_topology = self.topology
                        new_topology.remove(self.id)
                        self.log(logging.INFO, 'Initiating removal of ourselves from the topology.')
                        self.build_and_enqueue_message(self.successor_id, 'TOPOLOGY_UPDATE', new_topology)
                elif command == "status":
                    self.status()
                elif command == "delay":
                    self.set_delay(content)
                elif command == "count":
                    goal = int(content[0])
                    self.enqueue_task(goal)
                elif command == "send":
                    target_id = int(content[0])
                    message_text = " ".join(content[1:])  # capture full text
                    self.build_and_enqueue_message(target_id, 'TEXT', message_text)
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

    def send_message(self, target_id, message_payload: Message):
        """
        Sends a JSON-structured message to a target node, updating local lamport clock.
        """
        target_ip = ID_IP_MAP.get(target_id)
        target_port = 5000
        if not target_ip:
            self.log(logging.ERROR, f"Invalid target node ID: {target_id}")
            return

        # Build the JSON payload
        json_payload = json.dumps(message_payload)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((target_ip, target_port))
                client_socket.sendall(json_payload.encode("utf-8"))
                # Record the sent message into the map
                message_payload["time_sent"] = time.time()
                self.sent_messages[message_payload.get('message_id')] = message_payload
                self.log(logging.INFO, f"Sent message to Node {target_id}: {json_payload}")
        except Exception as e:
            self.log(logging.ERROR, f"Failed to send message to Node {target_id}: {e}")

    def build_and_enqueue_message(self, target_id, message_type, message_content=None, replying_to=None):
        """
        Builds a JSON message with structured fields:
        {
          "sender_id": <number>,
          "sender_clock": <number>,
          "message_id": "senderId-senderClock" or a UUID,
          "message_type": <string or undefined>,
          "message_content": <string or null>,
          "replying_to": <original_message_id or null>
        }
        """
        # Increment logical clock before sending
        with self.lock:
            self.logical_clock += 1
            sender_clock = self.logical_clock
            sender_id = self.id

        message_id = f"{sender_id}-{sender_clock}"
        message_payload = {
            "sender_id": sender_id,
            "sender_clock": sender_clock,
            "message_id": message_id,
            "message_type": message_type,
            "message_content": message_content,
            "replying_to": replying_to
        }
        scheduled_time = time.time() + self.delay
        self.outgoing_queue.put((target_id, message_payload, scheduled_time))
        self.log(logging.INFO,
                 f"Enqueued message to Node {target_id} at scheduled time {scheduled_time:.2f}")
        return message_id

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

    def process_topology_update(self, new_topology, sender_id):
        """
        1. If self.topology == new_topology, we assume the ring has fully received it
           (since it came back around) -> stop forwarding.
        2. If we are not in new_topology, it means we've been removed from ring -> set online=False or handle it.
        3. Otherwise update self.topology, update successor/predecessor,
           and forward the update to successor.
        """
        old_topology = self.topology[:] if self.topology else []

        if self.topology == new_topology:
            self.log(logging.INFO, f"Received TOPOLOGY_UPDATE from Node {sender_id}, "
                                   f"but we already have this topology = {new_topology}. "
                                   f"Stopping the ring update.")
            return

        # Are we removed?
        if self.id not in new_topology:
            self.log(logging.INFO, f"Our ID {self.id} is not in the new topology {new_topology}. "
                                   "We have been removed from the ring.")
            self.topology = None
            self.update_successor_and_predecessor()
            self.leave()
            return

        # Normal update: update local topology, predecessor/successor, forward along
        self.topology = new_topology
        self.topology.sort()
        self.update_successor_and_predecessor()

        self.log(logging.INFO,
                 f"TOPOLOGY_UPDATE from Node {sender_id}. Updated topology from {old_topology} to {new_topology}. "
                 f"My predecessor: {self.predecessor_id}, successor: {self.successor_id}.")

        # Forward the same update to successor if we have one
        if self.successor_id is not None and self.successor_id != self.id:
            # Build message content as JSON list
            forward_content = json.dumps(self.topology)
            self.build_and_enqueue_message(self.successor_id, "TOPOLOGY_UPDATE", forward_content)
            self.log(logging.INFO, f"Forwarding TOPOLOGY_UPDATE to Node {self.successor_id}")

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
                    replying_to = payload.get("replying_to")
                    self.log(logging.INFO, f"Received {message_type} message from Node {sender_id}: {message_content} (Message ID: {message_id})")
                    if replying_to is not None and replying_to in self.sent_messages:
                        original_message = self.sent_messages[replying_to]
                        payload['time_received'] = time.time()
                        self.received_replies[replying_to] = payload
                        duration = payload.get('time_received') - original_message.get('time_sent')
                        self.log(logging.INFO,f"Received message is a reply to our original {original_message.get('message_type')}. Roundtrip time {duration}ms.")

                    if message_type == 'JOIN':
                        self.build_and_enqueue_message(sender_id, 'JOIN_ACK', replying_to=message_id)
                        self.log(logging.INFO, f"Initiating TOPOLOGY_UPDATE of Node {sender_id}")
                        new_topology = self.topology[:]
                        new_topology.append(sender_id)
                        new_topology.sort()
                        self.process_topology_update(new_topology, sender_id)
                    if message_type == "TOPOLOGY_UPDATE":
                        # message_content is expected to be a JSON-serialized list of node IDs
                        try:
                            # Convert e.g. "[0,2,5]" to a Python list
                            updated_list = json.loads(message_content)
                            self.process_topology_update(updated_list, sender_id)
                        except Exception as e:
                            self.log(logging.WARNING, f"Invalid TOPOLOGY_UPDATE content: {message_content}, error={e}")
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
