import socket
import sys
import threading
import pickle
import random
import datetime
import time
from utils import *
from static import *
import logging
import time

# Static variables
BASE_PORT = 9000
REPLICATION_FACTOR = 4
WRITE_QUORUM = 3
READ_QUORUM = 1


class File_System:

    def __init__(self, MACHINE_NUM, LOGGER, MEM_LIST, STATUS):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.leader_node = None

        if self.MACHINE_NUM == 1:
            self.version = time.mktime(datetime.datetime.now().timetuple())
            self.nodeId = (self.ip, self.port, self.version)
            # Start with machine id == 1 as leader

        self.logger = LOGGER
        self.membership_list = MEM_LIST
        self.status = STATUS


    def send_message(self, sock_fd, msg, ip, port):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendto(msg, (ip, port))
        except:
            pass


    def leader_election(self):
        ''' Detect whether the leader has failed 
            If the leader has failed, start a new election
        '''
        while True:
            if self.leader_node == None or self.leader_node not in self.membership_list.active_nodes:
                machines = list(self.membership_list.active_nodes.keys())
                machine_ids = [machine[1] for machine in machines]
                min_machine_id_index = np.argmin(machine_ids)

                self.leader_node = machines[min_machine_id_index]
                self.logger.info(f"New leader elected: {self.leader_node}")

                # TODO: Send a message to all machines to update the leader
                sleep(8)


    def get(self, mssg):
        ''' Read a file from the SDFS '''
        if self.leader_node == self.nodeId:
            # If leader
            sdfsfilename = mssg.sdfsfilename

            if sdfsfilename not in self.membership_list.file_replication_dict:
                # No file found error
                msg = Message('Error', self.nodeId, None, None, error_message="File Does not Exist")
                self.send_message(sock_fd, pickle.dumps(msg), mssg.host[0], mssg.host[1])
            else:
                replica_dict = self.membership_list.file_replication_dict[sdfsfilename]
                replica_server = replica_dict['primary']  # Because READ_QUORUM = 1

                # TODO: Get from the appropriate replica server

        else:
            # If not leader, forward the message to the current leader
            self.send_message(sock_fd, data, self.leader_node[0], self.leader_node[1])



    def put(self, mssg):
        ''' Write a file to the SDFS '''
        if self.leader_node == self.nodeId:
            # If leader
            sdfsfilename = mssg.sdfsfilename

            if sdfsfilename not in self.membership_list.file_replication_dict:
                # TODO: Create a new file
                servers = self.membership_list.active_nodes.keys()
                replica_servers = random.sample(servers, REPLICATION_FACTOR)
                replica_dict = {'primary': replica_servers[0], 'secondary': replica_servers[1:]}

                # TODO: Send the file to the appropriate replica servers
            else:
                # TODO: Update the file
                replica_dict = self.membership_list.file_replication_dict[sdfsfilename]
                replica_servers = [replica_dict['primary']] + replica_dict['secondary']

        else:
            # If not leader, forward the message to the current leader
            self.send_message(sock_fd, data, self.leader_node[0], self.leader_node[1])


    def delete(self, mssg):
        ''' Delete a file from the SDFS '''
        if self.leader_node == self.nodeId:
            # If leader
            sdfsfilename = mssg.sdfsfilename

            if sdfsfilename not in self.membership_list.file_replication_dict:
                # No file found error
                msg = Message('Error', self.nodeId, None, None, error_message="File Does not Exist")
                self.send_message(sock_fd, pickle.dumps(msg), mssg.host[0], mssg.host[1])
            else:
                replica_dict = self.membership_list.file_replication_dict[sdfsfilename]
                replica_server = replica_dict['primary']

                # TODO: Delete from all the appropriate replica server

        else:
            # If not leader, forward the message to the current leader
            self.send_message(sock_fd, data, self.leader_node[0], self.leader_node[1])



    def start_machine(self):
        ''' Start the server '''
        leader_election_thread = threading.Thread(target=self.leader_election)

        leader_election_thread.start()
        leader_election_thread.join()
    


if __name__ == "__main__":
    MACHINE_NUM = sys.argv[1]

    machine = Machine(int(MACHINE_NUM))
    machine.start_machine()