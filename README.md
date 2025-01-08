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

## How to Start It Up

1. **Ensure IP Configuration**: 
   - Each node is associated with a fixed IP from the `ID_IP_MAP`. 
   - Update the `ID_IP_MAP` if necessary to match your local network settings. 
   - By default, the ring uses addresses `192.168.64.201` through `192.168.64.205`.

2. **Run the Node**:
   - Launch the node with `python3 node.py`.
   - The program will determine your local IP address and match it to one of the IPs in `ID_IP_MAP`.
   - On successful matching, the node starts up in *offline* mode (i.e., it is not yet part of the ring).

3. **CLI Interaction**:
   - Upon starting, the node presents a command prompt.
   - Type `join` to make the node *online* and probe potential successors in the ring.
   - Use `status` to see the node’s current state (topology info, logical clock, tasks, etc.).
   - Type `leave` to gracefully exit the ring.
   - Type `quit` (or press Ctrl+C) to fully shut down the node program.

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

### Additional Logging Controls

- You can toggle printing for each log category with commands like:
  - `+h` to enable printing of **HEARTBEAT** logs.
  - `-t` to disable printing of **TOPOLOGY** logs.
  - `.w` to enable only the **WORK TASK** category while disabling all others.

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
