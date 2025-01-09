import logging
import socket
import subprocess
import threading
import sys
import time
import queue
import json
import typing
from flask import Flask, jsonify, request

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
HEARTBEAT_SLEEP     = 1
HEARTBEAT_TIMEOUT   = 10
PROBING_TIMEOUT     = 3
MORE_WORK_TIMEOUT   = 3
CHECK_INTERVAL      = 1

log_file = "node.log"
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(levelname)s\tNode: %(node_id)s\tLogical Clock: %(clock)s\t%(message)s",
)
logger = logging.getLogger()
CAT_LABELS = {
    'h': 'HEARTBEAT',
    'm': 'MISRA',
    't': 'TOPOLOGY',
    'n': 'NETWORKING',
    'w': 'WORK TASK',
    'i': 'INTERNAL',
    'c': 'MESSAGING',
    'l': 'LOGICAL CLOCK',
    's': 'STATUS',
}

class Node:
    def __init__(self):
        self.log_categories = {key: True for key in CAT_LABELS.keys()}
        self.id = None
        self.logical_clock = 0
        self.ip = self.get_local_ip()
        self.port = 5000
        self.online = False
        self.delay = 0
        self.lock = threading.Lock()
        self.init_node_id_from_ip()
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
        self.predecessor_last_heartbeat_time = None
        self.my_heartbeat_thread = None
        self.monitor_predecessor_heartbeat_thread = None
        self.misra_process_color = 'black'
        self.misra_marker_present = False

        self.app = Flask(__name__)
        self.rest_api_thread = None
        self.setup_rest_routes()

    # IP ADDRESS AND NODE ID

    def get_local_ip(self):
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

    def init_node_id_from_ip(self):
        self.log('i', "Attempting to determine Node ID from IP.")
        for node_id, ip in ID_IP_MAP.items():
            if ip == self.ip:
                self.id = node_id
                break
        if self.id is None:
            self.log('i', "Failed to determine Node ID from IP.")
            sys.exit(1)
        self.log('i', f"Initialized as Node {self.id} with IP {self.ip}")

    def start_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.log('n', f"Server socket started on {self.ip}:{self.port}")

    # CONSOLE PRINTING AND LAMPORT CLOCK UTILITIES

    def log(self, cat, message, level = logging.INFO):
        cat_label = CAT_LABELS.get(cat, 'UNKNOWN')
        global logger
        logger.log(
            level,
            f"[{cat_label}] {message}",
            extra={
                "node_id": getattr(self, "id", "N/A"),
                "clock": getattr(self, "logical_clock", "N/A")
            }
        )
        if cat == 's' or (self.log_categories.get(cat, False)):
            console_line = f"{cat_label}\tN{self.id}\tC{self.logical_clock}\t{message}"
            print(console_line)

    def increase_logical_clock(self, reason, sender_clock=0):
        with self.lock:
            old_value = self.logical_clock
            self.logical_clock = max(self.logical_clock, sender_clock) + 1
            new_value = self.logical_clock
            self.log('l', f"Logical Clock increased {old_value}->{new_value}. Reason: {reason}")
        return new_value

    #  HEARTBEAT

    def start_heartbeat_checking_and_sending(self):
        self.log('h', "Starting heartbeat and predecessor monitor threads.")
        self.my_heartbeat_thread = threading.Thread(target=self._my_heartbeat_loop, daemon=True)
        self.my_heartbeat_thread.start()
        self.monitor_predecessor_heartbeat_thread = threading.Thread(target=self._monitor_predecessor_heartbeat_loop, daemon=True)
        self.monitor_predecessor_heartbeat_thread.start()

    def stop_heartbeat_checking_and_sending(self):
        if self.my_heartbeat_thread and self.my_heartbeat_thread.is_alive():
            self.my_heartbeat_thread.join(timeout=2)
        if self.monitor_predecessor_heartbeat_thread and self.monitor_predecessor_heartbeat_thread.is_alive():
            self.monitor_predecessor_heartbeat_thread.join(timeout=2)

    def _my_heartbeat_loop(self):
        self.log('h', "Heartbeat thread started.")
        while self.online:
            self.increase_logical_clock('my heart beated')
            if self.successor_id is not None:
                self.build_and_enqueue_message(self.successor_id, "HEARTBEAT", "ping")
                self.log('h', f"Sending HEARTBEAT to successor Node {self.successor_id}")
            else:
                self.log('h', "No successor - noone to send my heartbeat this round.")

            for _ in range(HEARTBEAT_INTERVAL):
                if not self.online:
                    break
                time.sleep(HEARTBEAT_SLEEP)

        self.log('h', "Heartbeat thread ended.")

    def _monitor_predecessor_heartbeat_loop(self):
        self.log('h', "Thread to monitor predecessor's heartbeat started.")
        while self.online:
            with self.lock:
                pred = self.predecessor_id
                last_hb = self.predecessor_last_heartbeat_time

            if pred is None:
                time.sleep(CHECK_INTERVAL)
                continue

            if last_hb and (time.time() - last_hb > HEARTBEAT_TIMEOUT):
                self.log('h',
                         f"Predecessor Node {pred} did not send any heartbeat in past {HEARTBEAT_TIMEOUT}s. Removing from ring.")
                if self.topology and pred in self.topology:
                    with self.lock:
                        new_topo = self.topology[:]
                    new_topo.remove(pred)
                    self.process_topology_update(new_topo, self.id)

            for _ in range(CHECK_INTERVAL):
                if not self.online:
                    break
                time.sleep(1)
        self.log('h', "Predecessor monitor thread ended.")

    def update_successor_and_predecessor(self):
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

        idx = self.topology.index(self.id)
        succ_idx = (idx + 1) % len(self.topology)
        pred_idx = (idx - 1) % len(self.topology)
        if self.predecessor_id != self.topology[pred_idx]:
            self.predecessor_last_heartbeat_time = time.time()
        self.successor_id = self.topology[succ_idx]
        self.predecessor_id = self.topology[pred_idx]

        self.log('t',f"My predecessor is Node {self.predecessor_id}, my successor is Node {self.successor_id}.")

    def accept_connections(self):
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
                self.log('c', f"Error accepting connection: {e}")
                break

    # JOINING (START NETWORKING, DETERMINE SUCCESSOR, STARTING WORK AND HEARTBEAT THREADS)

    def join(self):
        self.increase_logical_clock('joining the topology')
        self.online = True
        self.start_networking()
        self.probe_for_successor()
        self.start_work_processor()
        self.start_heartbeat_checking_and_sending()
        self.start_rest_api()
        self.log('t', "Node has joined the topology.")

    def start_networking(self):
        self.server_socket.listen(5)
        self.log('n', "Server socket is listening.")
        self.incoming_connections_thread = threading.Thread(target=self.accept_connections, daemon=True)
        self.incoming_connections_thread.start()
        self.outgoing_connections_thread = threading.Thread(target=self.process_outgoing_messages, daemon=True)
        self.outgoing_connections_thread.start()

    def probe_for_successor(self):
        self.log('t', "Node will now probe for successors.")
        self.topology = [self.id]  # start with only ourselves
        candidate_ids = [x for x in ID_IP_MAP.keys() if x != self.id]
        candidate_ids.sort()  # ascending order
        cand_id = (self.id + 1) % 5
        while cand_id != self.id:
            if cand_id == self.id:
                break
            self.log('t', f"Probing successor with id {cand_id}.")
            msg_id = self.build_and_enqueue_message(cand_id, "JOIN")
            start_wait = time.time()
            while time.time() - start_wait < PROBING_TIMEOUT:
                if msg_id in self.received_replies:
                    reply_info = self.received_replies[msg_id]
                    self.log('t', f"Successful probe to Node {reply_info.get('sender_id')} that is already in the topology.")
                    return  # done
                time.sleep(0.1)
            cand_id = (cand_id + 1) % 5
        self.log('t', f"No successors found online. We are alone in the topology: {self.topology}")
        self.successor_id = None

    def start_work_processor(self):
        self.log('w', "Starting work processor thread.")
        self.work_thread = threading.Thread(target=self.process_tasks, daemon=True)
        self.work_thread.start()

    # LEAVING (STOPPING NETWORKING AND REALATED THREADS)

    def leave(self):
        self.increase_logical_clock('leaving the topology')
        self.online = False
        self.stop_networking()
        self.stop_work_processor()
        self.stop_heartbeat_checking_and_sending()
        self.predecessor_id = None
        self.successor_id = None
        self.topology = None
        self.predecessor_last_heartbeat_time = None
        self.my_heartbeat_thread = None
        self.monitor_predecessor_heartbeat_thread = None
        self.misra_marker_present = False
        self.misra_process_color = 'black'
        self.log('t', "Node has left the topology.")

    def stop_networking(self):
        self.server_socket.shutdown(socket.SHUT_RDWR)
        self.log('n', "Server socket is shut down.")
        if self.incoming_connections_thread and self.incoming_connections_thread.is_alive():
            self.incoming_connections_thread.join(timeout=2)
        if self.outgoing_connections_thread and self.outgoing_connections_thread.is_alive():
            self.outgoing_connections_thread.join(timeout=2)

    def stop_work_processor(self):
        self.log('w', "Stopping work processor thread.")
        if self.work_thread and self.work_thread.is_alive():
            self.work_thread.join(timeout=2)

    def set_delay(self, content):
        self.log('i', "Delay change requested.")
        self.delay = content
        self.log('i', f"Delay set to: {self.delay}s.")

    # HANDLING COUNTING (WAITING) TASKS

    def process_tasks(self):
        while self.online:
            try:
                task_id, task_type, start, goal = self.work_queue.get(timeout=1)
                self.task_in_progress = (task_id, task_type)
                self.increase_logical_clock(f'processing task {task_id}')

                if task_type == "count":
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
        current_clock = self.increase_logical_clock(f'enqueuing new tak ({start}..{goal})')

        task_id = f"{self.id}-{current_clock}-({start}..{goal})"
        self.work_queue.put((task_id, "count", start, goal))
        self.log('w', f"Enqueued count task: {task_id} (range {start}..{goal}).")

    def handle_received_count_task(self, start: int, goal: int):
        total_count = goal - start + 1
        if total_count <= 0:
            self.log('w', f"Count task received invalid range [{start}..{goal}]. No tasks enqueued.")
            return

        self.increase_logical_clock(f'handling count task [{start}..{goal}]')

        if total_count <= 10:
            self.log('w', f"Enqueuing local count task from {start} to {goal}")
            self.enqueue_count_task(start, goal)
            return

        if self.successor_id is None:
            self.log('w', f"No successor. We'll split range into multiple local tasks.")
            current = start
            while current <= goal:
                chunk_end = min(current + 9, goal)
                self.log('w', f"Enqueuing chunk: count from {current} to {chunk_end}")
                self.enqueue_count_task(current, chunk_end)
                current = chunk_end + 1
            return

        local_end = start + 9
        self.log('w', f"Taking local chunk [start={start}, end={local_end}] for this node.")
        self.enqueue_count_task(start, local_end)

        new_start = local_end + 1
        if new_start <= goal:
            remainder_string = f"{new_start},{goal}"
            self.log('w', f"Delegating remainder [{new_start}..{goal}] to successor Node {self.successor_id}")
            self.build_and_enqueue_message(self.successor_id, "DELEGATE_COUNT", remainder_string)

    def request_more_work(self):
        with self.lock:
            pred = self.predecessor_id
        if pred is None:
            self.log('w', "No predecessor. Cannot request more work.")
            return

        msg_id = self.build_and_enqueue_message(pred, "REQUEST")
        self.log('w', f"Asking predecessor Node {pred} for more work, waiting for reply...")

        start_time = time.time()
        while time.time() - start_time < MORE_WORK_TIMEOUT:
            if msg_id in self.received_replies:
                reply_info = self.received_replies[msg_id]
                reply_type = reply_info["message_type"]
                reply_content = reply_info["message_content"]
                if reply_type == "REQUEST_ACK":
                    if reply_content:
                        try:
                            s, g = reply_content.split(",", 1)
                            start_int, goal_int = int(s), int(g)
                            self.log('w', f"Delegated range received: [{start_int}..{goal_int}]")
                            self.handle_received_count_task(start_int, goal_int)
                        except Exception as e:
                            self.log('w', f"Invalid range in REQUEST_ACK: {reply_content}, error: {e}")
                    else:
                        self.log('w', "Predecessor has no tasks to delegate.")
                return
            time.sleep(0.1)

        self.log('w', "Timeout waiting for REQUEST_ACK from predecessor.")

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
                    self.change_console_logging(line)
                    continue
                full_command = line.lower().split()
                command = full_command[0]
                if not self.assert_online_offline_commands(command):
                    continue

                if command == "join":
                    self.join()
                elif command == "leave":
                    self.log('t', 'Initiating removal of ourselves from the topology.')
                    if self.successor_id is not None:
                        new_topology = self.topology[:]
                        new_topology.remove(self.id)
                        self.build_and_enqueue_message(self.successor_id, 'TOPOLOGY_UPDATE', json.dumps(new_topology))
                    else:
                        self.leave()
                elif command == "status":
                    self.get_node_status()
                elif command == "delay":
                    self.set_delay(int(full_command[1]))
                elif command == "count":
                    self.handle_received_count_task(1, int(full_command[1]))
                elif command == "misra":
                    self.handle_misra(0)
                elif command == "quit":
                    self.log('i', "Node is shutting down per user request.")
                    sys.exit(0)
                else:
                    self.log('i', f"Unknown CLI command received: {command}")
            except KeyboardInterrupt:
                self.log('i', "Node is shutting down due to KeyboardInterrupt.")
                sys.exit(0)
            except EOFError:
                self.log('i', "CLI input ended unexpectedly (EOFError).")
                sys.exit(0)
            except Exception as e:
                self.log('i', f"Issue with command. Try again")

    def change_console_logging(self, line):
        sign = line[0]
        cat = line[1]
        word = 'Enabled' if sign == '+' else 'Disabled'
        if cat == 'a':
            if sign not in ('+', '-'):
                print(f"For turning all categories on/off please use '+a'/'-a'")
            for key in self.log_categories.keys():
                self.log_categories[key] = False if sign == '-' else True
                print(f"{word} printing for category '{CAT_LABELS[key]}'.")
        elif cat in self.log_categories:
            if sign == '+':
                self.log_categories[cat] = True
                print(f"{word} printing for category '{CAT_LABELS[cat]}'.")
            elif sign == '-':
                self.log_categories[cat] = False
                print(f"{word} printing for category '{CAT_LABELS[cat]}'.")
            elif sign == '.':
                self.log_categories[cat] = True
                print(f"{word} printing for category '{CAT_LABELS[cat]}'.")
                for key in self.log_categories.keys():
                    if key == cat:
                        continue
                    self.log_categories[key] = False
                    print(f"Disabled printing for category '{CAT_LABELS[key]}'.")

        else:
            print(f"Unknown category '{cat}'. Known are {list(self.log_categories.keys())}")

    def get_misra_status(self):
        return (
            f"Process Color: {self.misra_process_color.capitalize()}  Marker Present: {'Yes' if self.misra_marker_present else 'No'}"
        )

    def get_current_task_info(self):
        if self.task_in_progress:
            task_id, task_type, start, goal = self.task_in_progress
            return f"Task ID: {task_id}, Type: {task_type}, Range: ({start}..{goal})"
        else:
            return "None"

    def get_node_status(self):
        with self.lock:
            busy_state = "BUSY" if self.is_busy() else "IDLE"
            misra_status = self.get_misra_status()
            pending_tasks = self.work_queue.qsize()

            status_info = (
                "\n---- Node Status ----\n"
                f"Node ID          : {self.id}\n"
                f"IP Address       : {self.ip}\n"
                f"Online           : {'Yes' if self.online else 'No'}\n"
                f"Logical Clock    : {self.logical_clock}\n"
                f"Message Delay    : {self.delay}s\n"
                f"Work State       : {busy_state}\n"
                f"Topology         : {self.topology if self.topology else 'None'}\n"
                f"Predecessor ID   : {self.predecessor_id if self.predecessor_id is not None else 'None'}\n"
                f"Successor ID     : {self.successor_id if self.successor_id is not None else 'None'}\n"
                f"Pending Tasks    : {pending_tasks}\n"
                f"Misra Status     : {misra_status}\n"
                "----------------------"
            )

            self.log('s', status_info)

    # OUTGOING MESSAGING

    def build_and_enqueue_message(self, target_id, message_type, message_content=None, replying_to=None):
        sender_clock = self.increase_logical_clock('creating a new message')
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
        return message_id

    def send_message(self, target_id, message_payload: Message):
        target_ip = ID_IP_MAP.get(target_id)
        target_port = 5000
        if not target_ip:
            self.log('c', f"Invalid target node ID: {target_id}")
            return
        json_payload = json.dumps(message_payload)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((target_ip, target_port))
                client_socket.sendall(json_payload.encode("utf-8"))
                message_payload["time_sent"] = time.time()
                self.sent_messages[message_payload.get('message_id')] = message_payload
                self.log('c', f"Sent message to Node {target_id}: {json_payload}")
        except Exception as e:
            self.log('c', f"Failed to send message to Node {target_id}: {e}")

    def process_outgoing_messages(self):
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

    # INCOMING MESSAGING

    def parse_message(self, raw_data):
        try:
            payload = json.loads(raw_data)
            sender_clock = payload.get("sender_clock", 0)
            self.increase_logical_clock('received message, max(myClock, senderClock) + 1', sender_clock)
            return payload
        except json.JSONDecodeError:
            self.log('c', f"Failed to decode JSON: {raw_data}")
            return None
        except Exception as e:
            self.log('c', f"Error parsing message: {e}")
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
                    replying_to = payload.get("replying_to")
                    self.log('c', f"Received message from Node {sender_id}: {payload}")
                    self.misra_process_color = 'black'
                    if replying_to is not None and replying_to in self.sent_messages:
                        original_message = self.sent_messages[replying_to]
                        self.received_replies[replying_to] = payload
                        duration = time.time() - original_message.get('time_sent')
                        self.log('c',f"Received message is a reply to our original {original_message.get('message_type')}. Roundtrip time {duration}ms.")

                    if message_type == "DELEGATE_COUNT":
                        self.handle_incoming_delegate_count(sender_id, message_content)
                    elif message_type == 'JOIN':
                        self.handle_incoming_join_probe(sender_id, message_id)
                    elif message_type == "REQUEST":
                        self.handle_incoming_work_request(sender_id, message_id)
                    elif message_type == "TOPOLOGY_UPDATE":
                        self.handle_incoming_topology_update(message_content, sender_id)
                    elif message_type == "HEARTBEAT":
                        self.handle_incoming_heartbeat(sender_id)
                    elif message_type == "MARKER":
                        self.handle_incoming_marker(message_content)
                else:
                    self.log('c', f"Received invalid or unparseable message: {raw_message}")
        except Exception as e:
            self.log('c', f"Error handling incoming message: {e}")
        finally:
            conn.close()

    def handle_incoming_join_probe(self, sender_id, message_id):
        self.log('t', f"Received JOIN probe from Node {sender_id}. Replying with ACK")
        self.build_and_enqueue_message(sender_id, 'JOIN_ACK', replying_to=message_id)
        self.log('t', f"Initiating inclusion of Node {sender_id} into the ring.")
        new_topology = self.topology[:]
        new_topology.append(sender_id)
        new_topology.sort()
        self.process_topology_update(new_topology, sender_id)

    def handle_incoming_topology_update(self, message_content, sender_id):
        try:
            updated_list = json.loads(message_content)
            self.process_topology_update(updated_list, sender_id)
        except Exception as e:
            self.log('t', f"Invalid TOPOLOGY_UPDATE content: {message_content}, error={e}")

    def process_topology_update(self, new_topology, sender_id):
        old_topology = self.topology[:] if self.topology else []

        if self.topology == new_topology:
            self.log('t', f"Received TOPOLOGY_UPDATE from Node {sender_id}, "
                                   f"but we already have this topology = {new_topology}. "
                                   f"Stopping the ring update.")
            return

        self.increase_logical_clock(f'processing topology update from Node {sender_id}')

        if self.id not in new_topology:
            self.log('t', f"Our ID {self.id} is not in the new topology {new_topology}. "
                                   "We have been removed from the ring.")
            self.topology = None
            self.update_successor_and_predecessor()
            self.leave()
            return

        if self.successor_id is not None and self.successor_id not in new_topology:
            self.build_and_enqueue_message(self.successor_id, "TOPOLOGY_UPDATE", json.dumps(new_topology))
            self.log('t', f"Forwarding topology update to successor Node {self.successor_id}, who is leaving.")
        self.topology = new_topology
        self.topology.sort()
        self.update_successor_and_predecessor()

        self.log('t',
                 f"TOPOLOGY_UPDATE from Node {sender_id}. Updated topology from {old_topology} to {new_topology}. "
                 f"My predecessor: {self.predecessor_id}, successor: {self.successor_id}.")

        if self.successor_id is not None and self.successor_id != self.id:
            forward_content = json.dumps(self.topology)
            self.build_and_enqueue_message(self.successor_id, "TOPOLOGY_UPDATE", forward_content)
            self.log('t', f"Forwarding TOPOLOGY_UPDATE to Node {self.successor_id}")

    def handle_incoming_delegate_count(self, sender_id, message_content):
        try:
            s, g = message_content.split(",", 1)
            s_int, g_int = int(s), int(g)
            self.log('w',
                     f"Received DELEGATE_COUNT for range [{s_int}..{g_int}] from Node {sender_id}")
            self.handle_received_count_task(s_int, g_int)
        except Exception as e:
            self.log('w', f"Invalid DELEGATE_COUNT content: {message_content} - {e}")

    def handle_incoming_work_request(self, sender_id, message_id):
        delegated_start, delegated_goal = None, None
        try:
            item = self.work_queue.get_nowait()
            task_id, task_type, start, goal = item
            delegated_start, delegated_goal = start, goal
            self.log('t', f"Delegating local task range [{start}..{goal}] to Node {sender_id} via REQUEST_ACK.")
        except queue.Empty:
            self.log('t', f"I have no work to share. Not sending ACK to Node {sender_id}.")
        if delegated_start is not None:
            content_str = f"{delegated_start},{delegated_goal}"
        else:
            content_str = None
        self.build_and_enqueue_message(sender_id, "REQUEST_ACK", content_str, replying_to=message_id)

    def handle_incoming_heartbeat(self, sender_id):
        if sender_id == self.predecessor_id:
            with self.lock:
                self.predecessor_last_heartbeat_time = time.time()
            self.log('h', f"Heartbeat received from predecessor Node {sender_id}.")
        else:
            self.log('h',
                     f"Received HEARTBEAT from Node {sender_id}, not recognized as current predecessor.")
    def handle_incoming_marker(self, message_content):
        try:
            current_count = int(message_content)
        except ValueError:
            current_count = 0
        self.handle_misra(current_count)

    def handle_misra(self, current_count=0):
        self.increase_logical_clock('Handling MARKER for termination detection')
        if self.is_busy():
            self.log('m', "Marker arrived, but we are active. Will release the marker once we are idle.")
            self.misra_marker_present = True
        else:
            if self.successor_id is None:
                self.log('m',
                         f"Marker arrived and we are idle. Incrementing marker from 0 to 1.")
                self.log('m',
                         f"MARKER algorithm: Detected global termination! Count of processes in the ring (1) == count of contiguous white processes (1)")
                return
            self.log('m',
                     f"Marker arrived and we are idle. Incrementing marker from {current_count} to {current_count + 1}.")
            self.misra_process_color = 'white'
            current_count += 1
            if self.topology and current_count == len(self.topology):
                self.log('m',
                         f"MARKER algorithm: Detected global termination! Count of nodes in the ring ({len(self.topology)}) == count of contiguous white processes ({current_count})")
            elif self.successor_id is not None and len(self.topology) > 1:
                self.log('m',
                         f"Forwarding MARKER with count={current_count} to Node {self.successor_id}.")
                self.build_and_enqueue_message(self.successor_id, "MARKER", current_count)
            self.misra_marker_present = False

    def start(self):
        self.start_socket()
        self.handle_cli()
        self.log('i', f"Node {self.id} is ready to accept commands...")

    # REST API

    def setup_rest_routes(self):
        """Defines the REST API routes."""
        @self.app.route('/count/<int:goal>', methods=['GET'])
        def count(goal):
            if not self.online:
                return jsonify({"error": "Node is offline"}), 400
            try:
                self.handle_received_count_task(1, goal)
                return jsonify({"status": "count task enqueued", "goal": goal}), 200
            except Exception as e:
                self.log('i', f"Error handling /count/{goal}: {e}")
                return jsonify({"error": "Failed to enqueue count task"}), 400

        @self.app.route('/join', methods=['POST'])
        def join_route():
            if self.online:
                return jsonify({"error": "Node is already online"}), 400
            try:
                self.join()
                return jsonify({"status": "Node joined the topology"}), 200
            except Exception as e:
                self.log('i', f"Error handling /join: {e}")
                return jsonify({"error": "Failed to join the topology"}), 400

        @self.app.route('/leave', methods=['POST'])
        def leave_route():
            if not self.online:
                return jsonify({"error": "Node is already offline"}), 400
            try:
                self.leave()
                return jsonify({"status": "Node left the topology"}), 200
            except Exception as e:
                self.log('i', f"Error handling /leave: {e}")
                return jsonify({"error": "Failed to leave the topology"}), 400

        @self.app.route('/status', methods=['GET'])
        def status():
            try:
                self.get_node_status()
                return jsonify({"status": "Status information logged"}), 200
            except Exception as e:
                self.log('i', f"Error handling /status: {e}")
                return jsonify({"error": "Failed to retrieve status"}), 400

        @self.app.route('/delay/<int:seconds>', methods=['POST'])
        def set_delay_route(seconds):
            try:
                self.set_delay(seconds)
                return jsonify({"status": f"Message delay set to {seconds} seconds"}), 200
            except Exception as e:
                self.log('i', f"Error handling /delay/{seconds}: {e}")
                return jsonify({"error": "Failed to set delay"}), 400

        @self.app.route('/misra', methods=['POST'])
        def misra_route():
            try:
                self.handle_misra(0)
                return jsonify({"status": "Misra termination detection initiated"}), 200
            except Exception as e:
                self.log('i', f"Error handling /misra: {e}")
                return jsonify({"error": "Failed to initiate Misra termination detection"}), 400

    def start_rest_api(self):
        self.rest_api_thread = threading.Thread(target=self.run_rest_api, daemon=True)
        self.rest_api_thread.start()

    def run_rest_api(self):
        self.app.run(host='0.0.0.0', port=8080, threaded=True, use_reloader=False)


# Main Function
if __name__ == "__main__":
    node = Node()
    node.start()
