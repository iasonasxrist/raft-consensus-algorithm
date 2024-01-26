import random
import threading
import time
import logging
import os
import shutil

logging.basicConfig(level=logging.INFO)


class Node:
    
    """
        Initializes a node in a distributed system.

        Parameters:
        - id: Unique identifier for the node.
        - all_nodes: List of all nodes in the distributed system.
        - data: Simulated database for the node.
        - state: Initial state of the node; Starts as a 'follower'.
        - current_term: The current term of the node in the distributed system.
        - vote_for: The ID of the node voted for in the current term.
        - election_timeout: Randomized election timeout to avoid simultaneous elections.
        - all_nodes: List of all nodes in the distributed system.
        - stop_flag: Flag to signal the node to stop its activities (Threading Technique).
        - leader_id: The ID of the current 'leader' (initially set to None).
    """

    """
        Class variables to share the variable across all nodes!
    """
    shared_replica_logs_initialized = False
    leader_log_initialized = False

    def __init__(self, id, all_nodes):
        self.id = id
        self.data = {}
        self.state = 'follower'
        self.current_term = 0
        self.vote_for = None
        self.election_timer = random.uniform(1, 7)
        self.reset_election_timer()
        self.all_nodes = all_nodes
        self.stop_flag = threading.Event()
        self.leader_id = None
        self.leader_heartbeat_timeout = 10 # This is critical for your follower's election process
        self.last_communication_time = time.time()
        self.leader_election_lock = threading.Lock()
        self.leader_log_path = os.path.expanduser('~/Documents/System-Design-Projects/single-leader-replication/leader')
        self.replication_log_path = os.path.expanduser('~/Documents/System-Design-Projects/single-leader-replication/replicas')

        
  

    
    """
        Check if the file already exists before initializing
    """

    def initialize_replica_logs(self):
                

        self.replica_log_paths = { i : f'{self.replication_log_path}/replica_log_{i}.txt' for i in range(len(self.all_nodes))}

        # Create or clear replica log files if they don't exist
        for key, replica_log_path in self.replica_log_paths.items():
            if not os.path.exists(replica_log_path):
                try:
                    with open(replica_log_path, 'w') as f:
                        f.write('Init!\n')
                except FileNotFoundError:
                    print(f"The directory for {replica_log_path} does not exist")

        shared_replica_logs_initialized = True


    """
        Initialize the leader's log file.
    """
    def initialize_leader_logs(self):
        """
        Initialize the leader's log file.
        """
        if not Node.leader_log_initialized:
            self.leader_file_path = os.path.join(self.leader_log_path, f'leader_log_{self.id}.txt')

            # Check if the leader's log file already exists
            if not os.path.exists(self.leader_file_path):
                try:
                    with open(self.leader_file_path, 'w') as f:
                        f.write('Init Leader!\n')
                    Node.leader_log_initialized = True
                except Exception as e:
                    print(f"Error creating leader log file: {e}")



    """
        Move the ex-leader's log file to followers folder and vice versa 
    """


    def move_leader_logs(self):
        try:
            if self.state == 'leader':
                new_leader_log_path = os.path.join(self.leader_log_path, f'leader_log_{self.leader_id}.txt')
                os.rename(self.leader_file_path, new_leader_log_path)
                logging.info(f"Moved leader log to: {new_leader_log_path}")

                # If the previous leader's log exists, move it to the follower folder
                previous_leader_log_path = os.path.join(self.leader_log_path, f'leader_log_{self.id}.txt')
                if os.path.exists(previous_leader_log_path):
                    shutil.move(previous_leader_log_path, os.path.join(self.replication_log_path, f'replica_log_{self.id}.txt'))
                    logging.info(f"Moved previous leader log to follower folder: {os.path.join(self.replication_log_path, f'replica_log_{self.id}.txt')}")

            elif self.state == 'follower' and self.leader_id is not None:
                new_follower_log_path = os.path.join(self.replication_log_path, f'follower_log_{self.id}.txt')
                os.rename(self.leader_file_path, new_follower_log_path)
                logging.info(f"Moved follower log to: {new_follower_log_path}")

                # If the previous follower's log exists, move it to the follower folder
                previous_follower_log_path = os.path.join(self.leader_log_path, f'replica_log_{self.leader_id}.txt')
                if os.path.exists(previous_follower_log_path):
                    shutil.move(previous_follower_log_path, os.path.join(self.replication_log_path, f'replica_log_{self.leader_id}.txt'))
                    logging.info(f"Moved previous follower log to follower folder: {os.path.join(self.replication_log_path, f'replica_log_{self.leader_id}.txt')}")
        except FileNotFoundError:
            print(f"The directory for {self.leader_log_path} does not exist")


    """
        Random timer for Raft election timeout
    """

    def reset_election_timer(self):
        self.election_timer = time.time() + self.election_timer

    """
        Election check the state of node
        and the term if is updated in comparison with the others 
        (Updated nodes (terms) can be leader!!)
        Also a replication flag checks the first election to generate the txt files

    """

    def start_election(self):

        with self.leader_election_lock:
            if self.state != 'leader':
                # Reset the election timer
                self.reset_election_timer()


                self.remove_follower_replica_logs()

                # Increment the current term
                self.current_term += 1
                self.vote_for = self.id
                self.state = 'candidate'
                logging.info(f"Election timer reset for Node {self.id}")

                # Simulate sending RequestVote RPCs to other nodes
                # vote for itself
                votes_received = 1 

                for node in self.all_nodes:
                    if node.id != self.id:
                        if node.request_vote(self.current_term, self.id):
                            votes_received += 1

                # If majority of votes succeed
                if votes_received > len(self.all_nodes) // 2:
                    self.state = 'leader'
                    self.leader_id = self.id
                    self.initialize_leader_logs()
                    self.move_leader_logs()
                    

                    # Update the followers of leader's id
                    for node in self.all_nodes:
                        if node.id != self.id:
                            node.leader_id = self.id

                    
                
                logging.info(f"Node {self.id} elected as NEW Leader")

                """
                    Following it will runs only once!
                    After first election it will not run again!
                    Class variable keep tracked from all nodes
                """

                if not Node.shared_replica_logs_initialized:
                       
                        self.initialize_replica_logs()
                else:
                    # Election failed, revert back to follower state
                    self.state = 'follower'
                    logging.info(f"Node {self.id} remains follower")

    
    """ 
        Update node's term to the latest 
        A node in higher term always must be considered as leader
        If this happen, current node should revert to the 'follower' state.
    """

    def request_vote(self, term, candidate_id):
 
        if term > self.current_term:
            self.current_term = term
            self.vote_for = None  
            self.state = 'follower'
            self.reset_election_timer()
            self.heartbeat()
            
        
        """  
            If current node set as follower,
            we force it to vote the node with the higher term
            as implemented below
        """

        if self.state == 'follower' and (self.vote_for is None or self.vote_for == candidate_id):
            self.vote_for = candidate_id
            return True
        
        """
            If a message receive from the leader reset its clock
        """

        if self.state == 'follower':
            
            self.reset_election_timer()
            self.heartbeat()

        else:

            """ On this step,
                current node had already voted for leader 
                or it's on higher term
            """
            return False


        """ 
            Only the leader has the rights to write to the database
            So, it updates the replica sets and handling potential erros
        """

    def perform_database_operation(self):
        
        if self.state == 'leader':

            key = f"key_{random.randint(1, 10)}"
            value = f"value_{random.randint(1, 100)}"
            replication_log_message = f"Node {self.id} (Leader) wrote data: {key} -> {value}"

            # Write to leader log
            self.write_to_log_leader(self.leader_file_path, replication_log_message)
            logging.info(f"Node {self.id} (Leader) wrote data: {key} -> {value}")

            for node in self.all_nodes:
                if node.id != self.id:
                    try:
                        # Write to replica log for each follower
                        replica_log_path = self.replica_log_paths[node.id]
                        self.write_to_log_replica(replica_log_path, node.id, replication_log_message)
                        time.sleep(2)
                        # logging.info(f"Leader replicated data to Node {node.id}")

                    except Exception as e:
                        logging.error(f"Error replicating data to Node {node.id}: {e}")

            logging.info("Leader Replicate data to followers!")

        else:
            # Code for non-leader nodes
            logging.info(f"Node {self.id} (Non-Leader) unable to write data")


    def write_to_log_replica(self, log_path, replica_id, message):

        try:
            with open(log_path, 'a') as log_file:
                log_file.write(f"Replica {replica_id}: {message}\n")
        except FileNotFoundError:
            print(f"The directory for {log_path} does not exist")

    """
        Simulate the database action of write
    """

    def write_to_log_leader(self, leader_file_path, message):

        try:
            with open(leader_file_path, 'a') as log_file:
                log_file.write(f"{message}\n")
        except FileNotFoundError:
            print(f"The directory for {leader_file_path} does not exist")



    """
        Simulates RPC for each follower
    """

    def heartbeat(self):
        self.last_communication_time = time.time()

    def receive_hearbeat(self, leader_id):

        if self.state != 'leader' and leader_id == self.leader_id:
            self.heartbeat()
            logging.info("Heartbeated")
            
    def send_heartbeat(self, leader_id):

        for node in self.all_nodes:
            if node.id !=self.id:
                node.receive_hearbeat(leader_id)


    def remove_follower_replica_logs(self):

        if self.leader_id is not None:
            follower_replica_log = f'replica_log_{self.leader_id}.txt'
            follower_replica_log_path = os.path.join(self.replication_log_path, follower_replica_log)

            try:
                if os.path.exists(follower_replica_log_path):
                    os.remove(follower_replica_log_path)
                    logging.info(f"Removed follower's replica log: {follower_replica_log}")
                return
            except FileNotFoundError:
                logging.warning(f"Follower's replica log not found: {follower_replica_log}")
                return


    def run(self):
        """
            When a follower/candidate has exceed the election timeout
            and hasn't leader heartbeat,he should start election immediately
        """

        while not self.stop_flag.is_set():
            
            self.remove_follower_replica_logs()
            if self.state == 'follower' or self.state == 'candidate':
           
                if time.time() > self.election_timer and (time.time() - self.last_communication_time >= self.leader_heartbeat_timeout):
                    self.start_election()

            
            """
                Perform database operation
            """
            if self.state == 'leader':
                self.perform_database_operation()
                logging.info("*** Database operation completed ***")
                self.send_heartbeat(self.id)

    def stop(self):
        self.stop_flag.set()

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
time.sleep(2400)

# Stop the leader to check the election mode for remaining nodes
logging.warning("********Sequential stop of leaders ********")
for node, thread in zip(nodes, threads):
    if node.leader_id == node.id:
        node.stop()
        logging.warning(f"Node {node.id} stopped. Current state: {node.state}")

# Allow some time for the election to happen
time.sleep(0.5)

# # Stop the nodes
# for node, thread in zip(nodes, threads):
#     node.stop()

# # Wait for all threads to finish
# for node, thread in zip(nodes, threads):
#     thread.join()
#     logging.info(f"Node {node.id} stopped. Final state: {node.state}")

# logging.info("Simulation terminated.")

