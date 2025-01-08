# Ring Distributed Nodes with Misra Termination Detection

## Overview

This program implements a *distributed node* within a ring-based topology. Each node in the ring can accept tasks, send tasks to its successor, receive tasks from its predecessor, and exchange heartbeats to detect node failures. It also supports a *termination detection* algorithm (the **Misra** marker-based method). The primary goals and features of this program are:

- **Distributed Topology**: Nodes are arranged in a ring. Each node keeps track of its predecessor and successor.
- **Heartbeat Mechanism**: Nodes periodically send heartbeats to detect unreachable (failed) nodes.
- **Task Management**: Nodes accept counting tasks and can split or delegate these tasks to their successors.
- **Termination Detection**: The program uses a simple marker-based algorithm (inspired by Misra’s method) to detect whether all nodes in the ring have become idle.
- **Console-Based Interaction**: Users interact with each node by entering commands (e.g., `join`, `status`, `count`, `leave`) in a CLI environment.

---

## What Does This Program Solve?

In distributed environments, it is often necessary for processes to:

1. **Coordinate** with each other over a network (here, a ring) without a centralized manager.
2. **Detect** when certain processes fail or leave.
3. **Distribute** workloads (like counting tasks) and rebalance tasks when nodes arrive or depart.
4. **Determine** a *global termination condition* (i.e., when all nodes have finished their tasks).

Our node program addresses these challenges by providing:

- A **fault-tolerant** (ring) topology where each node tracks a single predecessor and successor.
- **Automated** detection of node failures via heartbeats.
- **Flexible** workload distribution, allowing tasks to be split and delegated.
- A **marker-based** termination detection algorithm to confirm whether all nodes are collectively idle.

---

## Startup Guide

This section walks you through setting up, configuring, and starting the distributed node program in a Linux environment. Follow these steps carefully to ensure successful deployment and testing of the program.

---

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

---

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

---

## Running the Program

1. **Start the Node**
   Run the startup script to pull the latest code and start the node:

`./start_node.sh`

   What happens:
   - The script automatically **git pulls** and **rebases** the latest version of the program.
   - It runs the program with **python3 node.py**.

2. **Program Execution**
   - The node will attempt to detect its local IP using the **`enp0s1`** interface and match it to an ID in the `ID_IP_MAP`.
   - Upon success, the node initializes and starts in **offline mode**, waiting for user commands.

---

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

---

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

---

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

---

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

---

### Summary of Commands

| Command              | Description                                                                                                  |
|----------------------|--------------------------------------------------------------------------------------------------------------|
| **`+<cat>`**         | Enable console printing for a category (e.g., `+w` or `+m`).                                                |
| **`-<cat>`**         | Disable console printing for a category (e.g., `-w`, `-m`).                                                |
| **`.`<cat>`**        | *Only* show `<cat>` (disables all other categories), e.g. `.m`.                                             |
| **`+a`** / **`-a`**  | Enable/disable **all** categories.                                                                          |

---

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

## Summary

This distributed node program forms a robust demonstration of how processes can coordinate in a ring topology, exchanging tasks and detecting failures. By providing commands for joining, leaving, counting tasks, and monitoring system-wide termination, it showcases essential concepts of distributed systems:

- **Concurrency and Locking** to manage shared data.  
- **Network-based Communication** (TCP sockets) for exchanging messages.  
- **Lamport Logical Clocks** for event ordering.  
- **Heartbeat and Failure Detection** to maintain ring integrity.  
- **Marker-Based Termination** to identify when all nodes become idle.

Its console-based CLI allows you to interact with individual nodes, controlling how they delegate tasks, verify ring consistency, and detect termination. Whether used as a teaching tool, demonstration, or foundation for more advanced distributed algorithms, this program highlights key principles of distributed systems in a straightforward, ring-based architecture.

