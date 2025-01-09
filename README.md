# Ring Distributed Nodes with Misra Termination Detection

## Overview

This program implements a *distributed node* within a ring-based topology. Each node in the ring can accept tasks, send tasks to its successor, receive tasks from its predecessor, and exchange heartbeats to detect node failures. It also supports a *termination detection* algorithm (the **Misra** marker-based method). The primary goals and features of this program are:

- **Distributed Topology**: Nodes are arranged in a ring. Each node keeps track of its predecessor and successor.
- **Heartbeat Mechanism**: Nodes periodically send heartbeats to detect unreachable (failed) nodes.
- **Task Management**: Nodes accept counting tasks and can split or delegate these tasks to their successors.
- **Termination Detection**: The program uses a simple marker-based algorithm (inspired by Misra’s method) to detect whether all nodes in the ring have become idle.
- **Console-Based Interaction**: Users interact with each node by entering commands (e.g., `join`, `status`, `count`, `leave`) in a CLI environment.

---

## Distributed Algorithm and Termination Detection

This section explores the implementation of our **distributed algorithm** and the **Misra marker-based termination detection** within our distributed node program. Understanding these components is essential for comprehending how the system efficiently distributes tasks and accurately detects when all nodes have completed their work.

### Counting Task Distribution

#### Overview

The `count <int>` command initiates a counting task where the node counts every second from 1 up to the specified number. To manage workloads effectively across the ring of nodes, the program employs a strategy to split large counting tasks and delegate portions of these tasks to successor nodes. This ensures balanced task distribution and prevents any single node from becoming a bottleneck.

#### How It Works

1. **Task Reception**:
   - When a node receives the `count <int>` command, it interprets this as a request to count from 1 to `<int>`.
   - The node evaluates whether the task range exceeds 10. If it does, the task is considered *big*.

2. **Local Task Handling**:
   - For *big* tasks, the node **splits** the range into smaller chunks of up to 10 units each.
   - The node **enqueues** the first chunk (e.g., 1-10) into its **local work queue**.
   - A dedicated **work processor thread** continuously monitors and processes tasks from this queue, executing the counting operation.

3. **Delegation to Successor**:
   - After handling the first chunk locally, the node **delegates** the remaining range (e.g., 11-20) to its **successor** in the ring.
   - This delegation involves sending a **`DELEGATE_COUNT`** message containing the remaining range to the successor node.
   - The successor node, upon receiving the message, repeats the process: handling a local chunk and delegating the rest if necessary.

4. **Single Node Scenario**:
   - If the ring consists of **only one node**, it cannot delegate tasks. Therefore, the node **splits** the range into multiple smaller chunks (each up to 10) and **enqueues** them into its own work queue for sequential processing.

5. **Task Completion and Work Request**:
   - Once a node completes all tasks in its local queue, it **requests more work** from its **predecessor**.
   - The predecessor node, if it has tasks in its queue, responds by delegating them to the requester.
   - This ensures continuous task distribution and processing across the ring.

#### Flow of Tasks

The task distribution flows **unidirectionally** around the ring, maintaining a smooth and orderly progression of tasks. Here's a simplified illustration:

1. User commands count 100 at Node 0
2. [Node 0] Enqueues 1-10 locally and delegates 11-100 to [Node 1]
3. [Node 1] Enqueues 11-20 locally and delegates 21-100 to [Node 2]
4. ... 
5. Transitively the tasks would get back to Node 0

Each node ensures that it handles its portion of the task efficiently while leveraging the ring structure to distribute workload seamlessly.

### Misra Marker-Based Termination Detection

#### Overview

In distributed systems, determining when the entire network has completed its tasks is non-trivial. To address this, our program implements **Misra's marker-based termination detection algorithm**, which allows nodes to detect global termination without centralized coordination.

#### Key Concepts

- **Process Color**:
  - Each node maintains a **process color**, which can be either **black** or **white**.
  - **Black** indicates that the node is active (processing tasks), while **white** indicates that the node is idle.

- **Marker Presence**:
  - Nodes track whether a **marker** is present in the system.
  - A marker is used to signify the initiation and tracking of termination detection.

#### How It Works

1. **Process Coloring**:
   - According to Misra's rule, **any message arrival at a node turns the node's color to black**.
   - Similarly, when a node receives a task and starts processing it, it paints itself black to indicate activity.

2. **Initiating Termination Detection**:
   - A **misra command** can be issued to start the termination detection process at any node.
   - Upon receiving the marker command, the node:
     - **Paints itself white** befif it is idle or prepares to paint itself white once it becomes idle.
     - **Sends a marker message** to its successor.
   
3. **Handling Markers**:
   - When a node receives a **marker message**:
     - If the node is **idle**:
       - It increments the **marker count** and **forwards** the marker to its successor.
       - It paints itself **white**.
     - If the node is **busy**:
       - It sets a flag (`self.misra_marker_present = True`) indicating that the marker should be released once it becomes idle.
   
4. **Releasing Markers**:
   - Once a busy node completes its current tasks and finds its work queue empty, it:
     - **Releases the marker** by sending it to its successor with an incremented count.
     - **Paints itself white**.
   
5. **Detecting Global Termination**:
   - The marker carries a **count** that represents how many nodes have been painted white in a row.
   - If a node detects that the marker has traversed **N white processes in a row**, where **N** is the total number of nodes in the ring, it **logs a global termination** message.
   - This condition indicates that all nodes are idle, and no further tasks are being processed or delegated.
   
`MARKER algorithm: Detected global termination! Count of processes in the ring (5) == count of contiguous white processes (5)`
   
#### Handling Edge Cases

- **Single Node Ring**:
  - If the ring consists of a single node, it handles the marker entirely on its own.
  - It increments the marker count and immediately detects termination if idle.

- **Dynamic Ring Changes**:
  - When nodes join or leave the ring, the topology is updated, and the termination detection adjusts accordingly.

#### Maintaining Consistency

- **Process State Synchronization**:
  - Nodes ensure that their process colors accurately reflect their current activity.
  - The use of flags (`self.misra_marker_present`) helps manage marker release even if tasks are dynamically added or removed.

- **Sequential Marker Passing**:
  - Markers pass in a single direction (e.g., clockwise) around the ring to maintain orderly termination detection.

---

## Startup Guide

This section walks you through setting up, configuring, and starting the distributed node program in a Linux environment. Follow these steps carefully to ensure successful deployment and testing of the program.

### Prerequisites

1. **Decide on the Number of Nodes**
   - Determine the number of devices (up to 5) that will participate in the distributed ring.

2. **Prepare Linux Machines**
   - Use Linux-based machines or virtual machines (e.g., Ubuntu or Debian). If you're using virtual machines, ensure each VM is properly networked.

3. **Assign Static IPv4 Addresses**
   - Assign static IP addresses to each device. The program uses the following default IP mappings:

     ```
     ID_IP_MAP = {
         0: "192.168.64.201",
         1: "192.168.64.202",
         2: "192.168.64.203",
         3: "192.168.64.204",
         4: "192.168.64.205",
     }
     ```

   - Ensure that:
     - Each device has one of these IPs assigned to its **enp0s1** interface.
     - All devices can **ping** each other successfully. This confirms network connectivity.

     > **Note**: If you wish to use a different network interface or IP addresses, modify the program's `ID_IP_MAP` and **`get_local_ip`** function accordingly.

### Package Installation

1. **Install Required Dependencies**
   Run the following commands to install the necessary software:
```
sudo apt-get update
sudo apt-get install git python3 python3-pip
```

2. **Clone the Repository**
   Download the program from GitHub:

`git clone https://github.com/MatyasUrban/DistributedTerminationDetection.git`

3. **Navigate to the Program Directory**
   Move into the cloned directory:

`cd DistributedTerminationDetection`


### Running the Program

1. **Start the Node**
   Run the startup script to pull the latest code and start the node:

`./start_node.sh`

   What happens:
   - The script automatically **git pulls** and **rebases** the latest version of the program.
   - It runs the program with **python3 node.py**.

2. **Program Execution**
   - The node will attempt to detect its local IP using the **`enp0s1`** interface and match it to an ID in the `ID_IP_MAP`.
   - Upon success, the node initializes and starts in **offline mode**, waiting for user commands.

### Key Considerations

1. **Network Configuration**
   - Ensure that all devices in the ring are on the same network.
   - Each device must have its **enp0s1** interface configured with one of the predefined IPs.
   - Use **ping** to verify connectivity between devices.

2. **Modifying Defaults**
   - To use a different interface, update the **`get_local_ip`** function:
     ```
     if "<desired-interface>" in line:
     ```
   - To use a different set of IP addresses, update the `ID_IP_MAP` dictionary.

3. **Linux Compatibility**
   - The scripts and instructions are tailored for Linux environments. Ensure you have the required permissions (e.g., `sudo`) for installations and network configurations.

### Next Steps

- After startup, refer to the **Working with the CLI and Logging** chapter for information on interacting with the program.
- Use commands like **join**, **status**, and **count** to explore the program's features.

---

## Working with the Program

### Core CLI Commands

- **join**  
  Makes the node *online* and attempts to find a successor in the ring. If found, the node updates the ring topology. Otherwise, the node remains alone (a single-node ring).

- **leave**  
  Gracefully leaves the ring, notifying other nodes if necessary. The node transitions back to an offline state.

- **status**  
  Prints details about the node: whether it is online, ring membership (predecessor, successor, etc.), tasks queued, Misra marker status, and more.

- **delay `<seconds>`**  
  Adjusts the artificial delay added to outgoing messages. Useful for simulating network latency.

- **count `<number>`**  
  Requests a new counting task up to `<number>`. If the node is busy or the count range is large, it may split and delegate tasks to its successor.

- **misra**  
  Starts or triggers the marker-based termination detection procedure on the ring. If every node in the ring is idle after a marker pass, the ring is considered terminated.

- **quit**  
  Immediately stops the node process (shutting down sockets, threads, etc.).

---

## Working with the CLI and Logging

One of the most distinctive features of this distributed node program is its **console-based CLI** coupled with an **adjustable logging system**. By default, *every log message* is written to `node.log` (in the same directory), but **what appears in the console** depends on how you configure log categories.

### Why Separate Console Logs from the Log File?

- **Log File** (`node.log`): Captures **all** details for diagnostics, debugging, and permanent records.  
- **Console Output**: Shows **only** the categories you enable (plus any *status* reports). This prevents the console from becoming overly cluttered when a node processes many messages or tasks.

### Example of Console Output

Here’s a sample of how verbose the console can get if **all** categories are enabled:

```
INTERNAL	N0	C49	Received CLI input: count 20
LOGICAL CLOCK	N0	C50	Logical Clock increased 49->50. Reason: handling count task [1..20]
WORK TASK	N0	C50	Taking local chunk [start=1, end=10] for this node.
LOGICAL CLOCK	N0	C51	Logical Clock increased 50->51. Reason: enqueuing new tak (1..10)
WORK TASK	N0	C51	Enqueued count task: 0-51-(1..10) (range 1..10).
WORK TASK	N0	C51	Delegating remainder [11..20] to successor Node 1
LOGICAL CLOCK	N0	C52	Logical Clock increased 51->52. Reason: processing task 0-51-(1..10)
WORK TASK	N0	C52	Executing count task 0-51-(1..10) for range 1..10
WORK TASK	N0	C53	Counting: 1/10
LOGICAL CLOCK	N0	C53	Logical Clock increased 52->53. Reason: creating a new message
MESSAGING	N0	C53	Sent message to Node 1: {"sender_id": 0, "sender_clock": 53, "message_id": "0-53", "message_type": "DELEGATE_COUNT", "message_content": "11,20", "replying_to": null}
LOGICAL CLOCK	N0	C56	Logical Clock increased 53->56. Reason: received message, max(myClock, senderClock)
MESSAGING	N0	C56	Received message from Node 2: {'sender_id': 2, 'sender_clock': 55, 'message_id': '2-55', 'message_type': 'HEARTBEAT', 'message_content': 'ping', 'replying_to': None}
HEARTBEAT	N0	C56	Heartbeat received from predecessor Node 2.
WORK TASK	N0	C56	Counting: 2/10
WORK TASK	N0	C56	Counting: 3/10
```

While it’s great to have all these details **logged**, having every category displayed on-screen can make the console quite busy. That’s where *log category toggles* come in.

### Log Categories

Internally, every log message is assigned a **category** (`cat`). The following table shows the available categories:

| Category Key | Console Label        | Typical Purpose                     |
|--------------|----------------------|-------------------------------------|
| **h**        | HEARTBEAT           | Heartbeat messages, ring updates, etc. |
| **m**        | MISRA               | Misra marker-based termination detection info. |
| **t**        | TOPOLOGY            | Topology changes (predecessor/successor updates). |
| **n**        | NETWORKING          | Socket initialization, connection acceptance, shutting down. |
| **w**        | WORK TASK           | Enqueuing tasks, task splitting, counting progress. |
| **i**        | INTERNAL            | User input commands, debug info, prompts. |
| **c**        | MESSAGING           | JSON messages sent or received between nodes. |
| **l**        | LOGICAL CLOCK       | Any increments to the Lamport clock. |
| **s**        | STATUS              | *Always printed* for `status` requests. |

#### Default Behavior
- **All categories** (`h, m, t, n, w, i, c, l`) are enabled for console output by default.
- **Every** log message is recorded in `node.log`, regardless of these toggles.

#### Toggling Categories

Inside the CLI, you can enable or disable category printing on-the-fly:

- **`+<cat>`**: Enable console printing for a category (e.g., `+w` → show **WORK TASK** logs).  
- **`-<cat>`**: Disable console printing for a category (e.g., `-w` → stop showing **WORK TASK** logs).  
- **`.`<cat>**: Enable **only** that category and disable all others (e.g., `.w` → only show **WORK TASK** logs).  

Additionally:

- **`+a`**: Enable **all** categories.
- **`-a`**: Disable **all** categories.

> **Note**: The **`s`** category (STATUS) is *always printed* if you run the `status` command, even if you toggle it off with `-s`. This ensures you can always see the status output.

### Examples of Category Toggle Commands

Below are some typical usage scenarios:

1. **Focus on Task Execution** `.w`

This disables all categories except **WORK TASK**, so the console only shows messages about tasks being enqueued, executed, split, etc.

2. **Add Termination Detection Logs**  `+m`

Now that `.w` restricted output to only **WORK TASK**, `+m` re-enables **MISRA** logs as well. So the console shows tasks + marker-based logs.

3. **Hide Heartbeat Noise**  `-h`

Suppose you were seeing many repeated **HEARTBEAT** lines. Disabling them lets you focus on other categories (still logs to `node.log`).

4. **Disable Everything**  `-a`

This stops *all categories* from printing in the console. You’ll still see `status` outputs, but general logs won’t appear on-screen.

### Summary of Commands

| Command              | Description                                                                                                  |
|----------------------|--------------------------------------------------------------------------------------------------------------|
| **`+<cat>`**         | Enable console printing for a category (e.g., `+w` or `+m`).                                                |
| **`-<cat>`**         | Disable console printing for a category (e.g., `-w`, `-m`).                                                |
| **`.`<cat>`**        | *Only* show `<cat>` (disables all other categories), e.g. `.m`.                                             |
| **`+a`** / **`-a`**  | Enable/disable **all** categories.                                                                          |

## Putting It All Together

- **Log File**: Captures **every** log event. Always consult `node.log` if you need the full story.
- **Console**: Displays only the categories you want to see.  
- **Node CLI**: Accepts commands for toggling categories, so you can dynamically reduce noise or add detail.

Use these tools to strike the right balance between **visibility** and **noise** in your console output.

---

## Threads

Each node starts several threads to manage concurrency:

1. **CLI Thread**  
   - Responsible for reading user commands from the console and executing them.  
   - Runs the `handle_cli` function until the user quits or the node shuts down.

2. **Incoming Connections Thread**  
   - Listens for new TCP connections from other nodes on the ring (i.e., when they send tasks, heartbeats, or other messages).

3. **Outgoing Connections Thread**  
   - Processes a queue of outgoing messages.  
   - Each message is eventually delivered to the target node (successor, predecessor, or another node).

4. **Work Processor Thread**  
   - Handles local tasks (e.g., counting from 1 to N).  
   - If tasks are large, it may delegate portions to its successor.

5. **Heartbeat Thread**  
   - Periodically sends heartbeat messages to the successor.  
   - Updates the local logical clock each time a heartbeat is sent.

6. **Predecessor Monitor Thread**  
   - Monitors whether the predecessor has sent a heartbeat recently.  
   - If not, the node assumes the predecessor is dead or has left, and updates the ring topology accordingly.

---

## Example Workflow

1. **Start Node** `python3 node.py`

The node starts, remains offline, and waits for CLI commands.

2. **Join the Ring** `join`
- The node attempts to discover a successor using its IP-based ID.  
- If another node is online, it receives a response and updates its topology.  
- If no node responds, it becomes a single-node ring.

3. **Create a Counting Task** `count 20`
- The node enqueues a task to count up to 20.
- If it has a successor, it may delegate part of this task to lighten its load.

4. **Check Status** `status`
- Prints out a multi-line status showing whether the node is busy, how many tasks are queued, its predecessor, successor, etc.

5. **Test Termination** `misra`
- Initiates the marker-based termination detection.
- If every node in the ring is idle, the algorithm recognizes system-wide completion and reports that at one node.

6. **Leave the Ring**  `leave`
- The node gracefully notifies others, updates the ring so it’s removed, and goes offline.
- It can later re-join if desired (by re-running `join`).

---

## Technical Considerations

1. **Python Features**:
- **Threads and Locks** (`threading` and `lock`) for concurrency and shared data protection.  
- **Sockets** (`socket`) for TCP-based communication among nodes.  
- **Queues** (`queue.Queue`) to asynchronously process outgoing messages.  
- **Subprocess** for local IP detection (i.e., calling `ip a` on Linux).  
- **JSON** (`json`) for encoding/decoding messages between nodes.

2. **Lamport Logical Clock**:
- Each event (internal, send, or receive) increments this logical clock to maintain a partial ordering of events across distributed nodes.

3. **Ring Topology Updates**:
- Nodes track a sorted list (`self.topology`) of IDs.  
- Each node’s position determines its successor and predecessor.  
- On changes (e.g., a node leaving/failing), the ring updates and propagates the new topology.

4. **Failure Detection**:
- Heartbeats are sent to the successor at configurable intervals.  
- A node monitors its predecessor for timely heartbeats.  
- If the heartbeat times out, the node drops the predecessor and updates the ring.

5. **Misra’s Marker Algorithm**:
- A node that is idle can initiate the marker if it suspects global termination.  
- The marker visits each node in the ring, verifying whether the node is still idle.  
- If the marker completes a full cycle with every node remaining idle, the ring is considered terminated.

---

## Node's Internal State

Each node in the distributed system maintains a comprehensive internal state to manage its operations effectively. This state encompasses configurations, networking details, task management, messaging, and termination detection mechanisms. Understanding these attributes is essential for grasping how each node interacts within the ring topology and ensures seamless distributed processing.

### General Attributes

- **Logging Configuration**: Determines which categories of logs are printed to the console, allowing users to control the verbosity of the output without affecting the detailed logs stored in `node.log`.
- **Identification and Networking**: Each node has a unique identifier and IP address, facilitating communication within the ring.
- **Operational Status**: Tracks whether the node is active (`online`) and manages any artificial delays in message transmission.
- **Concurrency Management**: Utilizes locks and multiple threads to handle simultaneous operations safely.
- **Topology Management**: Maintains information about the ring's current structure, including predecessor and successor nodes.
- **Task Management**: Manages a queue of tasks to be processed, ensuring efficient distribution and execution of workloads.
- **Messaging System**: Handles the sending and receiving of messages between nodes, tracking both sent messages and received replies.
- **Heartbeat Mechanism**: Monitors the health of predecessor nodes to detect failures or disconnections.
- **Termination Detection**: Implements Misra's marker-based algorithm to determine when all nodes have completed their tasks, signaling global termination.

### Detailed Attributes

1. **`log_categories`**
   - **Type**: `Dict[str, bool]`
   - **Description**: Controls which log categories are printed to the console. Each key corresponds to a specific log category (e.g., 'h' for HEARTBEAT), and the boolean value determines whether logs of that category are displayed.

2. **`id`**
   - **Type**: `int`
   - **Description**: The unique identifier for the node, determined based on its IP address from the predefined `ID_IP_MAP`.

3. **`logical_clock`**
   - **Type**: `int`
   - **Description**: Implements Lamport's logical clock to maintain a partial ordering of events across the distributed system. It increments based on internal events and message exchanges.

4. **`ip`**
   - **Type**: `str`
   - **Description**: The local IPv4 address of the node, obtained by inspecting the `enp0s1` network interface. This address is crucial for establishing communication with other nodes.

5. **`port`**
   - **Type**: `int`
   - **Default**: `5000`
   - **Description**: The port number on which the node listens for incoming TCP connections from other nodes.

6. **`online`**
   - **Type**: `bool`
   - **Default**: `False`
   - **Description**: Indicates whether the node is currently active and part of the ring topology. An online node participates in task distribution and heartbeat exchanges.

7. **`delay`**
   - **Type**: `int`
   - **Default**: `0`
   - **Description**: An artificial delay (in seconds) added to outgoing messages to simulate network latency or control message flow.

8. **`lock`**
   - **Type**: `threading.Lock`
   - **Description**: A mutex lock ensuring thread-safe access to shared resources, such as the logical clock and task queues.

9. **`server_socket`**
   - **Type**: `socket.socket`
   - **Description**: The server socket bound to the node's IP and port, responsible for accepting incoming connections from other nodes.

10. **`outgoing_queue`**
    - **Type**: `queue.Queue`
    - **Description**: A thread-safe queue holding outgoing messages that need to be sent to other nodes. Messages are processed asynchronously by the outgoing connections thread.

11. **`outgoing_connections_thread`**
    - **Type**: `threading.Thread`
    - **Description**: A dedicated thread that processes messages from the `outgoing_queue` and handles the sending of these messages to target nodes.

12. **`incoming_connections_thread`**
    - **Type**: `threading.Thread`
    - **Description**: A dedicated thread that continuously listens for and accepts incoming connections on the `server_socket`. Each incoming connection is handled in a separate daemon thread.

13. **`topology`**
    - **Type**: `List[int]`
    - **Description**: A list maintaining the current ring topology by storing the IDs of all active nodes in ascending order.

14. **`successor_id`**
    - **Type**: `Optional[int]`
    - **Description**: The ID of the node's immediate successor in the ring. Used for sending tasks and heartbeats.

15. **`predecessor_id`**
    - **Type**: `Optional[int]`
    - **Description**: The ID of the node's immediate predecessor in the ring. Used for receiving delegated tasks and monitoring heartbeats.

16. **`work_queue`**
    - **Type**: `queue.Queue`
    - **Description**: A thread-safe queue holding tasks that the node needs to process. Tasks are typically counting operations that the node executes sequentially.

17. **`work_thread`**
    - **Type**: `threading.Thread`
    - **Description**: A dedicated thread that continuously monitors and processes tasks from the `work_queue`. It handles task execution and delegates remaining work as necessary.

18. **`task_in_progress`**
    - **Type**: `Optional[Tuple[str, str]]`
    - **Description**: Stores information about the current task being processed, including the task ID and type. This helps in tracking active operations and managing task completion.

19. **`sent_messages`**
    - **Type**: `Dict[str, Message]`
    - **Description**: A dictionary tracking all messages sent by the node, keyed by their unique message IDs. This is useful for correlating sent messages with received replies.

20. **`received_replies`**
    - **Type**: `Dict[str, Message]`
    - **Description**: A dictionary storing replies received for messages that the node has sent. This facilitates handling acknowledgments and responses from other nodes.

21. **`predecessor_last_heartbeat_time`**
    - **Type**: `Optional[float]`
    - **Description**: Timestamp of the last heartbeat received from the predecessor node. Used to detect failures or unresponsive nodes.

22. **`my_heartbeat_thread`**
    - **Type**: `threading.Thread`
    - **Description**: A dedicated thread that periodically sends heartbeat messages to the successor node to signal the node's active status.

23. **`monitor_predecessor_heartbeat_thread`**
    - **Type**: `threading.Thread`
    - **Description**: A dedicated thread that monitors the `predecessor_last_heartbeat_time`. If heartbeats from the predecessor are not received within the `HEARTBEAT_TIMEOUT`, the node assumes the predecessor has failed and updates the topology accordingly.

24. **`misra_process_color`**
    - **Type**: `str`
    - **Default**: `'black'`
    - **Description**: Indicates the node's current color in the termination detection algorithm. `'black'` signifies that the node is active or has processed a marker, while `'white'` indicates idleness.

25. **`misra_marker_present`**
    - **Type**: `bool`
    - **Default**: `False`
    - **Description**: A flag indicating whether a termination marker is currently present and awaiting release. If the node is busy when a marker arrives, this flag ensures the marker is released once the node becomes idle.


---

## Server and Flask Endpoints

Each node in the ring runs a **Flask** server on **port 8080**, providing a set of RESTful API endpoints for external interaction and automation. These endpoints allow you to manage node operations programmatically, enabling actions such as joining the ring, initiating tasks, adjusting message delays, and monitoring node status.

### Available Endpoints

| **Endpoint**             | **HTTP Method** | **Description**                                                         |
|--------------------------|-----------------|-------------------------------------------------------------------------|
| `/join`                  | `POST`          | Brings the node online and attempts to join the ring topology.          |
| `/leave`                 | `POST`          | Gracefully removes the node from the ring and sets it to offline mode.   |
| `/count/<int:goal>`      | `POST`          | Enqueues a counting task that counts from 1 up to the specified `<goal>`.|
| `/status`                | `GET`           | Logs and returns the current status of the node, including topology and tasks. |
| `/delay/<int:seconds>`   | `POST`          | Sets an artificial delay (in seconds) for outgoing messages to simulate network latency. |
| `/misra`                 | `POST`          | Initiates the Misra marker-based termination detection process.         |

### Endpoint Details

#### 1. **Join the Ring**

- **URL:** `/join`
- **Method:** `POST`
- **Description:** Activates the node and attempts to integrate it into the existing ring topology. If other nodes are online, the node will update its successor and predecessor accordingly. If no other nodes are present, it becomes the sole member of the ring.

#### 2. **Leave the Ring**

- **URL:** `/leave`
- **Method:** `POST`
- **Description:** Removes the node from the ring gracefully, updating the ring's topology to exclude this node. The node transitions to an offline state and stops participating in task distribution and heartbeat exchanges.

#### 3. **Enqueue a Counting Task**

- **URL:** `/count/<int:goal>`
- **Method:** `POST`
- **Description:** Initiates a counting task where the node counts from 1 up to the specified `<goal>`. The task may be split and delegated to successor nodes if the range exceeds 10 to balance the workload across the ring.

- **Parameters:**
  - `<int:goal>`: The upper limit of the counting task.

#### 4. **Check Node Status**

- **URL:** `/status`
- **Method:** `GET`
- **Description:** Retrieves and logs the current status of the node, including its ID, IP address, online status, logical clock value, message delay, work state (busy or idle), current topology, predecessor and successor IDs, pending tasks, and Misra termination detection status.

#### 5. **Set Message Delay**

- **URL:** `/delay/<int:seconds>`
- **Method:** `POST`
- **Description:** Adjusts the artificial delay added to outgoing messages. This feature is useful for simulating network latency or controlling the flow rate of messages between nodes.

- **Parameters:**
  - `<int:seconds>`: The number of seconds to set as the delay for outgoing messages.

#### 6. **Initiate Misra Termination Detection**

- **URL:** `/misra`
- **Method:** `POST`
- **Description:** Starts the Misra marker-based termination detection process. This process determines whether all nodes in the ring have become idle, indicating that the system has completed all tasks and reached global termination.

## Summary

This distributed node program forms a robust demonstration of how processes can coordinate in a ring topology, exchanging tasks and detecting failures. By providing commands for joining, leaving, counting tasks, and monitoring system-wide termination, it showcases essential concepts of distributed systems:

- **Concurrency and Locking** to manage shared data.  
- **Network-based Communication** (TCP sockets) for exchanging messages.  
- **Lamport Logical Clocks** for event ordering.  
- **Heartbeat and Failure Detection** to maintain ring integrity.  
- **Marker-Based Termination** to identify when all nodes become idle.

Its console-based CLI allows you to interact with individual nodes, controlling how they delegate tasks, verify ring consistency, and detect termination. Whether used as a teaching tool, demonstration, or foundation for more advanced distributed algorithms, this program highlights key principles of distributed systems in a straightforward, ring-based architecture.

