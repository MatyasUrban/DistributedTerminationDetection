import logging
import socket
import subprocess
import threading
import sys
import time
import queue
import json
import typing

# --- CONSTANTS ---
class Message(typing.TypedDict):
    sender_id: int
    sender_clock: int
    message_id: str
    message_type: str
    message_content: typing.Optional[str]
    replying_to: typing.Optional[str]

ID_IP_MAP = {
    0: "192.168.64.201",
    1: "192.168.64.202",
    2: "192.168.64.203",
    3: "192.168.64.204",
    4: "192.168.64.205",
}

HEARTBEAT_INTERVAL  = 5
HEARTBEAT_TIMEOUT   = 10
PROBING_TIMEOUT     = 3
MORE_WORK_TIMEOUT   = 3
CHECK_INTERVAL      = 1

# --- LOGGING CONFIGURATION ---
log_file = "node.log"
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(levelname)s\tNode: %(node_id)s\tLogical Clock: %(clock)s\t%(message)s",
)
logger = logging.getLogger()

# --- NODE CLASS ---
class Node:
    def __init__(self):
        self.CATEGORY_LABELS = {
            'h': 'HEARTBEAT',
            'm': 'MISRA',
            't': 'TOPOLOGY',
            'n': 'NETWORKING',
            'w': 'WORK',
            'i': 'INTERNAL',
            'c': 'MESSAGING'
        }
        self.log_categories = {key: True for key in self.CATEGORY_LABELS.keys()}
        """Initializes the node with default values and state."""
        self.id = None
        self.logical_clock = 0
        self.ip = self.get_local_ip()
        self.port = 5000
        self.online = False
        self.delay = 0
        self.lock = threading.Lock()
        self.init_node_id()
        self.server_socket = None


        # Core components
        self.outgoing_queue = queue.Queue()
        self.outgoing_connections_thread = None
        self.incoming_connections_thread = None

        # Topology and work processing
        self.topology = None
        self.successor_id = None
        self.predecessor_id = None
        self.work_queue = queue.Queue()
        self.work_thread = None
        self.task_in_progress = None

        # Communication tracking
        self.sent_messages = {}
        self.received_replies = {}

        # Heartbeat monitoring
        self.predecessor_last_heartbeat_time = None
        self.my_heartbeat_thread = None
        self.monitor_predecessor_heartbeat_thread = None

        # Misra termination detection
        self.misra_process_color = 'black'
        self.misra_marker_present = False

    # --- CORE INITIALIZATION AND NETWORKING ---

    def get_local_ip(self):
        """Detects the local IP address."""
        try:
            self.log('i', "Attempting to determine local IP address.")
            result = subprocess.run(["ip", "a"], capture_output=True, text=True, check=True)
            lines = result.stdout.splitlines()

            interface_found = False
            for line in lines:
                if "enp0s1" in line:
                    interface_found = True
                if interface_found and "inet " in line:
                    ip_address = line.strip().split()[1].split('/')[0]
                    self.log('n', f"Local IP address determined: {ip_address}")
                    return ip_address
            raise ValueError("enp0s1 interface not found or no IPv4 address assigned.")
        except Exception as e:
            self.log('n', f"Failed to determine local IP address: {e}")
            return "127.0.0.1"

    def init_node_id(self):
        """Maps the local IP address to the corresponding node ID."""
        self.log('i', "Attempting to determine Node ID from IP.")
        for node_id, ip in ID_IP_MAP.items():
            if ip == self.ip:
                self.id = node_id
                break
        if self.id is None:
            self.log('i', "Failed to determine Node ID from IP.")
            sys.exit(1)
        self.log('i', f"Initialized as Node {self.id} with IP {self.ip}")

    def log(self, cat, message, level = logging.INFO):
        cat_labels = {
            'h': 'HEARTBEAT',
            'm': 'MISRA',
            't': 'TOPOLOGY',
            'n': 'NETWORKING',
            'w': 'WORK',
            'i': 'INTERNAL',
            'c': 'MESSAGING'
        }
        cat_label = cat_labels.get(cat, 'UNKNOWN')
        global logger
        logger.log(
            level,
            f"[{cat_label}] {message}",
            extra={
                "node_id": getattr(self, "id", "N/A"),
                "clock": getattr(self, "logical_clock", "N/A")
            }
        )
        if self.log_categories and self.log_categories.get(cat, False):
            console_line = f"{cat_label}\tN{self.id}\tC{self.logical_clock}\t{message}"
            print(console_line)

    def _heartbeat_loop(self):
        """
        Sends heartbeats to successor every HEARTBEAT_INTERVAL seconds,
        if we have a successor and are online.
        """
        self.log('h', "Heartbeat thread started.")
        while self.online:
            with self.lock:
                succ = self.successor_id

            if succ is not None:
                # Enqueue a heartbeat
                self.build_and_enqueue_message(succ, "HEARTBEAT", "ping")
                self.log('h', f"Sending HEARTBEAT to successor Node {succ}")
            else:
                self.log('h', "No successor - skipping heartbeat this round.")

            # Sleep for the interval
            for _ in range(HEARTBEAT_INTERVAL):
                if not self.online:
                    break
                time.sleep(1)

        self.log('h', "Heartbeat thread ended.")

    def _predecessor_monitor_loop(self):
        """
        Monitors that predecessor's last heartbeat is not older than HEARTBEAT_TIMEOUT.
        If it is, we remove the predecessor from our ring and do a ring update,
        or handle it as you prefer.
        """
        self.log('h', "Thread to monitor predecessor's heartbeat started.")
        while self.online:
            with self.lock:
                pred = self.predecessor_id
                last_hb = self.predecessor_last_heartbeat_time

            if pred is None:
                # No predecessor right now, just wait a bit
                time.sleep(CHECK_INTERVAL)
                continue

            # If we haven't received a heartbeat from predecessor in too long
            if last_hb and (time.time() - last_hb > HEARTBEAT_TIMEOUT):
                self.log('h',
                         f"Predecessor Node {pred} missed heartbeat in {HEARTBEAT_TIMEOUT}s. Removing from ring.")
                # Remove predecessor from ring if it's in self.topology
                if self.topology and pred in self.topology:
                    new_topo = self.topology[:]
                    new_topo.remove(pred)
                    self.process_topology_update(new_topo, self.id)

            # Sleep short intervals
            for _ in range(CHECK_INTERVAL):
                if not self.online:
                    break
                time.sleep(1)

        self.log('h', "Predecessor monitor thread ended.")

    def update_successor_and_predecessor(self):
        """
        Sets self.successor_id and self.predecessor_id based on self.topology.
        If topology has only one node (ourselves), both successor_id and predecessor_id become None.
        """
        if self.topology is None:
            self.successor_id = None
            self.predecessor_id = None
            self.log('t', f"Removing successor and predecessor given that we were removed from topology.")
            return
        if len(self.topology) == 1:
            self.successor_id = None
            self.predecessor_id = None
            self.log('t', f"Removing successor and predecessor given that we are the only node in the topology.")
            return

        # We assume self.id is in self.topology. We'll find our index.
        idx = self.topology.index(self.id)
        # successor is (idx+1) mod len(topology)
        succ_idx = (idx + 1) % len(self.topology)
        pred_idx = (idx - 1) % len(self.topology)
        if self.predecessor_id != self.topology[pred_idx]:
            self.predecessor_last_heartbeat_time = time.time()
        self.successor_id = self.topology[succ_idx]
        self.predecessor_id = self.topology[pred_idx]

        self.log('t',f"My predecessor is Node {self.predecessor_id}, my successor is Node {self.successor_id}.")

    def accept_connections(self):
        """Accepts incoming connections and handles them."""
        self.log('n', "Server is now accepting connections.")
        while self.online:
            try:
                conn, addr = self.server_socket.accept()
                if not self.online:  # If the server is offline, ignore the connection
                    conn.close()
                    break
                threading.Thread(target=self.handle_incoming_message, args=(conn,), daemon=True).start()
            except OSError:
                self.log('n', "Server socket has been closed; stopping connection acceptance.")
                break
            except Exception as e:
                break

    def process_outgoing_messages(self):
        """Processes outgoing messages from the queue and sends them."""
        while self.online:
            try:
                target_id, message_payload, scheduled_time = self.outgoing_queue.get(timeout=1)

                now = time.time()
                if now < scheduled_time:
                    time_to_wait = scheduled_time - now
                    time.sleep(time_to_wait)
                self.send_message(target_id, message_payload)

            except queue.Empty:
                continue
            except Exception as e:
                if self.online:
                    self.log('c', f"Error processing outgoing message: {e}")

    def start_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.log('n', f"Server socket started on {self.ip}:{self.port}")

    def start_networking(self):
        self.server_socket.listen(5)
        self.log('n', "Server socket is listening.")
        self.incoming_connections_thread = threading.Thread(target=self.accept_connections, daemon=True)
        self.incoming_connections_thread.start()
        self.outgoing_connections_thread = threading.Thread(target=self.process_outgoing_messages, daemon=True)
        self.outgoing_connections_thread.start()

    def probe_for_successor(self):
        """
        Probes potential successors in ascending ID order (skipping self.id).
        If any replies with e.g. 'JOIN_ACK', we set that node as successor.
        Otherwise, if no replies, we consider ourselves alone in the topology.
        """
        self.log('t', "Node will now probe for successors.")
        self.topology = [self.id]  # start with only ourselves
        candidate_ids = [x for x in ID_IP_MAP.keys() if x != self.id]
        candidate_ids.sort()  # ascending order
        cand_id = (self.id + 1) % 5
        while cand_id != self.id:
            if cand_id == self.id:
                break
            self.log('t', f"Probing successor with id {cand_id}.")
            # Send a JOIN message
            msg_id = self.build_and_enqueue_message(cand_id, "JOIN")

            # Wait up to 3 seconds or break sooner if we see a reply
            start_wait = time.time()
            while time.time() - start_wait < PROBING_TIMEOUT:
                if msg_id in self.received_replies:
                    reply_info = self.received_replies[msg_id]
                    self.log('t', f"Successful probe to Node {reply_info.get('sender_id')} that is already in the topology.")
                    return  # done
                time.sleep(0.1)  # small wait, re-check
            cand_id = (cand_id + 1) % 5

        # If we reach here, no one responded
        self.log('t', f"No successors found online. We are alone in the topology: {self.topology}")
        self.successor_id = None  # or maybe set it to self.id if you want a ring of one
    def start_heartbeat_checking_and_sending(self):
        self.log('h', "Starting heartbeat and predecessor monitor threads.")
        self.my_heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.my_heartbeat_thread.start()
        self.monitor_predecessor_heartbeat_thread = threading.Thread(target=self._predecessor_monitor_loop, daemon=True)
        self.monitor_predecessor_heartbeat_thread.start()

    def join(self):
        self.online = True
        self.start_networking()
        self.start_work_processor()
        self.probe_for_successor()
        self.start_heartbeat_checking_and_sending()
        self.log('t', "Node has joined the topology.")

    def stop_networking(self):
        """Stops the networking components and closes the server socket."""
        self.server_socket.shutdown(socket.SHUT_RDWR)
        self.log('n', "Server socket is shut down.")
        if self.incoming_connections_thread and self.incoming_connections_thread.is_alive():
            self.incoming_connections_thread.join(timeout=2)
        if self.outgoing_connections_thread and self.outgoing_connections_thread.is_alive():
            self.outgoing_connections_thread.join(timeout=2)

    def leave(self):
        with self.lock:
            self.online = False
            self.stop_networking()
            self.stop_work_processor()
            self.successor_id = None
            self.topology = None
            self.predecessor_last_heartbeat_time = None
            self.my_heartbeat_thread = None
            self.monitor_predecessor_heartbeat_thread = None
            self.log('t', "Node has left the topology.")


    def status(self):
        with (self.lock):  # Ensure thread-safe access
            self.log('i', "Status requested.")
            busy_state = "BUSY" if self.is_busy() else "IDLE"
            status_info = f"Node ID: {self.id}, IP Address: {self.ip}, Online: {self.online}, Logical Clock: {self.logical_clock}, Message Delay: {self.delay}s, Work State: {busy_state}, Topology: {self.topology}, Predecessor: {self.predecessor_id}, Successor: {self.successor_id}\n"
            self.log('i', "Status: " + status_info)
    def set_delay(self, content):
        self.log('i', "Delay change requested.")
        self.delay = int(content[0])
        self.log('i', f"Delay set to: {self.delay}s.")

    def process_tasks(self):
        """Continuously processes tasks from the work queue, one at a time."""
        while self.online:
            try:
                # Wait for a task to become available
                task_id, task_type, start, goal = self.work_queue.get(timeout=1)
                self.task_in_progress = (task_id, task_type)
                if task_type == "count":
                    # Perform counting from 1 to task_goal
                    self.log('w', f"Executing count task {task_id} for range {start}..{goal}")
                    for i in range(start, goal + 1):
                        if not self.online:
                            self.log('w', "Node went offline, throwing current task.")
                            break
                        self.log('w', f"Counting: {i}/{goal}")
                        time.sleep(1)  # Simulate work

                    self.log('w', f"Task {task_id} completed.")

                self.task_in_progress = None
                if self.work_queue.empty():
                    self.request_more_work()
                if self.work_queue.empty() and self.misra_marker_present:
                    self.handle_misra()
                self.work_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                self.log('w', f"Error processing task: {e}")
                self.task_in_progress = None

    def enqueue_count_task(self, start: int, goal: int):
        with self.lock:
            self.logical_clock += 1
            current_clock = self.logical_clock

        task_id = f"{self.id}-{current_clock}-({start}..{goal})"
        self.work_queue.put((task_id, "count", start, goal))
        self.log('w', f"Enqueued count task: {task_id} (range {start}..{goal}).")

    def handle_count_task(self, start: int, goal: int):
        """
        Decides how to handle a counting range [start..goal].
        If (goal - start + 1) <= 10 => enqueue locally.
        If there's no successor but the range is > 10 => we split locally into multiple tasks of up to 10 each.
        If we do have a successor => take only up to 10 for local, delegate remainder to successor.
        """
        total_count = goal - start + 1
        if total_count <= 0:
            self.log('w', f"Count task received invalid range [{start}..{goal}]. No tasks enqueued.")
            return

        # If range size <= 10, just enqueue one local counting task.
        if total_count <= 10:
            self.log('w', f"Enqueuing local count task from {start} to {goal}")
            self.enqueue_count_task(start, goal)
            return

        # If no successor, we do everything locally but in chunks of up to 10
        if not self.successor_id:
            self.log('w', f"No successor. We'll split range into multiple local tasks.")
            current = start
            while current <= goal:
                chunk_end = min(current + 9, goal)
                self.log('w', f"Enqueuing chunk: count from {current} to {chunk_end}")
                self.enqueue_count_task(current, chunk_end)
                current = chunk_end + 1
            return

        # Otherwise, we have a successor. Take the first 10 locally; delegate the rest.
        local_end = start + 9
        self.log('w', f"Taking local chunk [start={start}, end={local_end}] for this node.")
        self.enqueue_count_task(start, local_end)

        # Delegate the remainder to successor
        new_start = local_end + 1
        if new_start <= goal:
            remainder_string = f"{new_start},{goal}"
            self.log('w', f"Delegating remainder [{new_start}..{goal}] to successor Node {self.successor_id}")
            self.build_and_enqueue_message(self.successor_id, "DELEGATE_COUNT", remainder_string)

    def request_more_work(self):
        """
        Sends a REQUEST message to predecessor, if any, and waits up to 3s for a 'REQUEST_ACK' reply.
        If the reply has a range in message_content (like 'start,goal'), we call handle_count_task on it.
        """
        with self.lock:
            pred = self.predecessor_id
        if not pred:
            self.log('w', "No predecessor. Cannot request more work.")
            return

        # Build & send the REQUEST message
        msg_id = self.build_and_enqueue_message(pred, "REQUEST")
        self.log('w', f"Asking predecessor Node {pred} for more work, waiting for reply...")

        # Wait up to 3s for a reply
        start_time = time.time()
        while time.time() - start_time < MORE_WORK_TIMEOUT:
            if msg_id in self.received_replies:
                reply_info = self.received_replies[msg_id]
                reply_type = reply_info["message_type"]
                reply_content = reply_info["message_content"]
                if reply_type == "REQUEST_ACK":
                    if reply_content:
                        # e.g. "15,25"
                        try:
                            s, g = reply_content.split(",", 1)
                            start_int, goal_int = int(s), int(g)
                            self.log('w', f"Delegated range received: [{start_int}..{goal_int}]")
                            self.handle_count_task(start_int, goal_int)
                        except Exception as e:
                            self.log('w', f"Invalid range in REQUEST_ACK: {reply_content}, error: {e}")
                    else:
                        self.log('w', "Predecessor has no tasks to delegate.")
                return
            time.sleep(0.1)

        self.log('w', "Timeout waiting for REQUEST_ACK from predecessor.")

    def start_work_processor(self):
        """Starts the worker thread that processes tasks (e.g., counting)."""
        self.log('w', "Starting work processor thread.")
        self.work_thread = threading.Thread(target=self.process_tasks, daemon=True)
        self.work_thread.start()
    def stop_work_processor(self):
        """Starts the worker thread that processes tasks (e.g., counting)."""
        self.log('w', "Stopping work processor thread.")
        if self.work_thread and self.work_thread.is_alive():
            self.work_thread.join(timeout=2)

    def is_busy(self):
        in_progress = (self.task_in_progress is not None)
        queue_non_empty = not self.work_queue.empty()
        return in_progress or queue_non_empty
    def assert_online_offline_commands(self, command):
        online_commands = ['leave', 'count', 'misra']
        offline_commands = ['join']
        if command in online_commands and not self.online:
            self.log('i', 'You must firstly join the topology before using this command.')
            return False
        if command in offline_commands and self.online:
            self.log('i', 'You must firstly leave the topology before using this command.')
            return False
        return True

    def handle_cli(self):
        """Handles CLI commands in a dedicated thread."""
        self.log('i', "NODE CLI IS READY")
        while True:
            try:

                line = input().strip()
                self.log('i', f"Received CLI input: {line}")
                if (line.startswith(('+', '-', '.'))) and len(line) == 2:
                    sign = line[0]
                    cat = line[1]
                    if cat in self.log_categories:
                        if sign == '+':
                            self.log_categories[cat] = True
                            print(f"Enabled printing for category '{cat}'.")
                        elif sign == '-':
                            self.log_categories[cat] = False
                            print(f"Disabled printing for category '{cat}'.")
                        elif sign == '.':
                            for key in self.log_categories.keys():
                                self.log_categories[key] = False
                            self.log_categories[cat] = True
                    else:
                        print(f"Unknown category '{cat}'. Known are {list(self.log_categories.keys())}")
                    continue
                full_command = line.lower().split()
                command = full_command[0]
                if not self.assert_online_offline_commands(command):
                    continue
                content = None
                if len(full_command)>1:
                    content = full_command[1:]
                elif command == "join":
                    self.join()
                elif command == "leave":
                    self.log('t', 'Initiating removal of ourselves from the topology.')
                    if self.successor_id:
                        new_topology = self.topology[:]
                        new_topology.remove(self.id)
                        self.build_and_enqueue_message(self.successor_id, 'TOPOLOGY_UPDATE', json.dumps(new_topology))
                    else:
                        self.leave()
                elif command == "status":
                    self.status()
                elif command == "delay":
                    self.set_delay(content)
                elif command == "count":
                    self.handle_count_task(1, int(content[0]))
                elif command == "misra":
                    self.handle_misra(-1)
                elif command == "quit":
                    self.log('i', "Node is shutting down via CLI.")
                    sys.exit(0)
                else:
                    self.log('i', f"Unknown CLI command received: {command}")
            except KeyboardInterrupt:
                self.log('i', "Node is shutting down due to KeyboardInterrupt.")
                print("\nShutting down node.")
                sys.exit(0)
            except EOFError:
                self.log('i', "CLI input ended unexpectedly (EOFError).")
                print("\nCLI input ended. Exiting.")
                sys.exit(0)
            except Exception as e:
                self.log('i', f"Unhandled exception in CLI: {e}")
                print(f"Error: {e}")
                sys.exit(1)

    def send_message(self, target_id, message_payload: Message):
        """
        Sends a JSON-structured message to a target node, updating local lamport clock.
        """
        target_ip = ID_IP_MAP.get(target_id)
        target_port = 5000
        if not target_ip:
            self.log('c', f"Invalid target node ID: {target_id}")
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
                self.log('c', f"Sent message to Node {target_id}: {json_payload}")
        except Exception as e:
            self.log('c', f"Failed to send message to Node {target_id}: {e}")

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
        self.log('c',
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
            self.log('c', f"Failed to decode JSON: {raw_data}")
            return None
        except Exception as e:
            self.log('c', f"Error parsing message: {e}")
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
            self.log('t', f"Received TOPOLOGY_UPDATE from Node {sender_id}, "
                                   f"but we already have this topology = {new_topology}. "
                                   f"Stopping the ring update.")
            return

        # Are we removed?
        if self.id not in new_topology:
            self.log('t', f"Our ID {self.id} is not in the new topology {new_topology}. "
                                   "We have been removed from the ring.")
            self.topology = None
            self.update_successor_and_predecessor()
            self.leave()
            return

        if self.successor_id not in new_topology:
            self.build_and_enqueue_message(self.successor_id, "TOPOLOGY_UPDATE", json.dumps(new_topology))
            self.log('t', f"Forwarding topology update to successor Node {self.successor_id}, who is leaving.")
        # Normal update: update local topology, predecessor/successor, forward along
        self.topology = new_topology
        self.topology.sort()
        self.update_successor_and_predecessor()

        self.log('t',
                 f"TOPOLOGY_UPDATE from Node {sender_id}. Updated topology from {old_topology} to {new_topology}. "
                 f"My predecessor: {self.predecessor_id}, successor: {self.successor_id}.")

        # Forward the same update to successor if we have one
        if self.successor_id is not None and self.successor_id != self.id:
            # Build message content as JSON list
            forward_content = json.dumps(self.topology)
            self.build_and_enqueue_message(self.successor_id, "TOPOLOGY_UPDATE", forward_content)
            self.log('t', f"Forwarding TOPOLOGY_UPDATE to Node {self.successor_id}")

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
                    self.log('c', f"Received {message_type} message from Node {sender_id}: {message_content} (Message ID: {message_id})")
                    self.misra_process_color = 'black'
                    if replying_to is not None and replying_to in self.sent_messages:
                        original_message = self.sent_messages[replying_to]
                        payload['time_received'] = time.time()
                        self.received_replies[replying_to] = payload
                        duration = payload.get('time_received') - original_message.get('time_sent')
                        self.log('c',f"Received message is a reply to our original {original_message.get('message_type')}. Roundtrip time {duration}ms.")
                    if message_type == "DELEGATE_COUNT":
                        # message_content is e.g. "15,30"
                        try:
                            s, g = message_content.split(",", 1)
                            s_int, g_int = int(s), int(g)
                            self.log('w',
                                     f"Received DELEGATE_COUNT for range [{s_int}..{g_int}] from Node {sender_id}")
                            self.handle_count_task(s_int, g_int)
                        except Exception as e:
                            self.log('w', f"Invalid DELEGATE_COUNT content: {message_content} - {e}")
                    if message_type == 'JOIN':
                        self.build_and_enqueue_message(sender_id, 'JOIN_ACK', replying_to=message_id)
                        self.log('t', f"Initiating TOPOLOGY_UPDATE of Node {sender_id}")
                        new_topology = self.topology[:]
                        new_topology.append(sender_id)
                        new_topology.sort()
                        self.process_topology_update(new_topology, sender_id)
                    if message_type == "REQUEST":
                        delegated_start, delegated_goal = None, None
                        try:
                            item = self.work_queue.get_nowait()
                            task_id, task_type, start, goal = item
                            delegated_start, delegated_goal = start, goal
                            self.log('t',
                                     f"Delegating local task range [{start}..{goal}] to Node {sender_id} via REQUEST_ACK.")
                        except queue.Empty:
                            pass

                        if delegated_start is not None:
                            content_str = f"{delegated_start},{delegated_goal}"
                        else:
                            content_str = None

                        self.build_and_enqueue_message(sender_id, "REQUEST_ACK", content_str, replying_to=message_id)
                        self.log('t', f"Sent REQUEST_ACK to Node {sender_id} with content='{content_str}'.")

                    if message_type == "TOPOLOGY_UPDATE":
                        # message_content is expected to be a JSON-serialized list of node IDs
                        try:
                            # Convert e.g. "[0,2,5]" to a Python list
                            updated_list = json.loads(message_content)
                            self.process_topology_update(updated_list, sender_id)
                        except Exception as e:
                            self.log('t', f"Invalid TOPOLOGY_UPDATE content: {message_content}, error={e}")
                    if message_type == "HEARTBEAT":
                        # If the sender is our current predecessor, update predecessor_last_heartbeat
                        if sender_id == self.predecessor_id:
                            with self.lock:
                                self.predecessor_last_heartbeat_time = time.time()
                            self.log('h', f"Heartbeat received from predecessor Node {sender_id}.")
                        else:
                            self.log('h',
                                     f"Received HEARTBEAT from Node {sender_id}, not recognized as current predecessor.")
                    if message_type == "MARKER":
                        try:
                            current_count = int(message_content)  # e.g. "0", "1", "2"
                        except ValueError:
                            current_count = 0
                        self.handle_misra(current_count)




                else:
                    self.log('c', f"Received invalid or unparseable message: {raw_message}")
        except Exception as e:
            self.log('c', f"Error handling incoming message: {e}")
        finally:
            conn.close()

    def start(self):
        self.start_socket()
        self.handle_cli()
        self.log('i', f"Node {self.id} is ready to accept commands...")


    def handle_misra(self, current_count=0):
        if self.is_busy():
            self.log('m', "Marker arrived, but we are active. Will release the marker once we are idle.")
            self.misra_marker_present = True
        else:
            if not self.successor_id:
                self.log('m',
                         f"Marker arrived and we are idle. Incrementing marker from 0 to 1.")
                self.log('m',
                         f"MARKER algorithm: Detected global termination! Count of processes in the ring ({len(self.topology)}) == count of contiguous white processes ({current_count})")
                return
            self.log('m',
                     f"Marker arrived and we are idle. Incrementing marker from {current_count} to {current_count + 1}.")
            self.misra_process_color = 'white'
            current_count += 1
            if self.topology and current_count == len(self.topology):
                self.log('m',
                         f"MARKER algorithm: Detected global termination! Count of processes in the ring ({len(self.topology)}) == count of contiguous white processes ({current_count})")
            elif self.successor_id is not None and len(self.topology) > 1:
                self.log('m',
                         f"Forwarding MARKER with count={current_count} to Node {self.successor_id}.")
                self.build_and_enqueue_message(self.successor_id, "MARKER", current_count)
            self.misra_marker_present = False


# Main Function
if __name__ == "__main__":
    node = Node()
    node.start()
