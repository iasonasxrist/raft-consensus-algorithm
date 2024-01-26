import time
import logging
from single_leader import Node
import threading

def test_node_crash_during_leader_election():
    # Simulate a node crash during leader election
    # For example, stop a node just after it starts an election
    crashed_node = nodes[0]
    crashed_node.stop()
    time.sleep(0.5)  # Allow time for the election to complete

    # Check if a new leader is elected
    for node in nodes:
        logging.info(f"Node {node.id} state after crash: {node.state}")



def test_network_partition():
    # Simulate a network partition by preventing communication between some nodes
    partitioned_nodes = nodes[:2]  # Nodes 0 and 1 are in one partition
    other_nodes = nodes[2:]  # Nodes 2, 3, and 4 are in the other partition

    for node in partitioned_nodes:
        node.stop()

    time.sleep(2)  # Allow time for the nodes to react to the partition

    # Check if the partitioned nodes and the other nodes behave correctly
    for node in nodes:
        logging.info(f"Node {node.id} state after network partition: {node.state}")


def test_leader_crash_during_database_operation():
    # Wait for some time before stopping the leader for demonstration purposes
    time.sleep(5)

    # Identify the current leader
    current_leader = None
    for node in nodes:
        if node.leader_id == node.id:
            current_leader = node
            break

    if current_leader:
        # Stop the current leader
        current_leader.stop()
        logging.info(f"Leader Node {current_leader.id} stopped. Current state: {current_leader.state}")
    else:
        logging.warning("No leader found to stop.")
    
    time.sleep(2)  # Allow time for the nodes to react and elect a new leader

    # Check if a new leader is elected
    for node in nodes:
        logging.info(f"Node {node.id} state after leader crash: {node.state}")

def test_multiple_simultaneous_leader_election_attempts():
    # Simulate multiple nodes starting leader election attempts simultaneously
    for node in nodes:
        threading.Thread(target=node.start_election).start()

    time.sleep(2)  # Allow time for the leader election attempts to complete

    # Check if a single leader is elected
    for node in nodes:
        logging.info(f"Node {node.id} state after simultaneous leader election attempts: {node.state}")

# Create a simple network of nodes
nodes = [Node(i, []) for i in range(5)]

for node in nodes:
    node.all_nodes = nodes

# Start the nodes in separate threads
threads = [threading.Thread(target=node.run) for node in nodes]

for thread, node in zip(threads, nodes):
    thread.start()
    logging.info(f"Node {node.id} started. Current state: {node.state}")

# Allow the simulation to run for some time
time.sleep(2)

logging.info(f"Starting Testing")


# Run tests
test_node_crash_during_leader_election()
test_leader_crash_during_database_operation()
test_network_partition()
test_multiple_simultaneous_leader_election_attempts()

# Stop the nodes
for node in nodes:
    node.stop()

# Wait for all threads to finish
for thread, node in zip(threads, nodes):
    thread.join()
    logging.info(f"Node {node.id} stopped. Final state: {node.state}")

logging.info("Simulation terminated.")
