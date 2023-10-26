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
import os
import numpy as np
import tqdm

# Static variables
BASE_PORT = 9000
REPLICATION_FACTOR = 2
WRITE_QUORUM = 3
READ_QUORUM = 1
MAX = 8192

BUFFER_SIZE = 4096
SEPARATOR = " "


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

        self.sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


    def send_message(self, sock_fd, msg, ip, port):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendto(msg, (ip, port))
        except:
            pass


    def leader_election(self, sock_fd):
        ''' Detect whether the leader has failed 
            If the leader has failed, start a new election
        '''
        while True:
            if self.leader_node == None or self.leader_node not in self.membership_list.active_nodes: # condition for running leader election
                machines = list(self.membership_list.active_nodes.keys())
                machine_ids = [machine[1] for machine in machines]
                min_machine_id_index = np.argmin(machine_ids)

                self.leader_node = machines[min_machine_id_index]
                self.logger.info(f"New leader elected: {self.leader_node}")


                # TODO: Send a message to all machines to update the leader
                # host = (self.ip, self.port, self.version)
                # msg = Message(msg_type='leader_update', 
                #               host=host, 
                #               membership_list=self.membership_list, 
                #               counter=self.ping_counter
                #              )
                # self.send_leader_msg(msg, sock_fd)
                time.sleep(8)


    def get(self, mssg, sock_fd):
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



    def put(self, local_filename, sdfs_filename, sock_fd):
        """Client machine forwards put message to leader"""
        if self.leader_node != self.nodeId:
            # If not leader, forward the message to the current leader
            mssg = FileSystemMessage(msg_type="put", 
                                     leader_host=self.leader_node, 
                                     origin_host=self.host,
                                     origin_filename=local_filename,
                                     dest_filename=sdfs_filename
                                    )
            
            self.send_message(sock_fd, mssg, self.leader_node[0], self.leader_node[1])


    def delete(self, mssg, sock_fd):
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


    def server_read_write(self):
        """receive messages from leader and act accordingly (get/put/delete)"""
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.port))

        while True:
            data, server = sock_fd.recvfrom(MAX) # receive serialized data from another machine
            
            if self.status == 'Joined':
                if data:
                    mssg = pickle.loads(data)

                    if mssg.type == 'put':
                        # machine needs to read a file from it's local fs and write it to the replicas mentioned by the leader in the msg
                        self.write_replicas(mssg)
                    elif mssg.type == 'get':
                        self.read_from_replica(mssg)
    
    def write_replicas(mssg):
        leader = mssg.leader_host
        replica_list = mssg.replica_list
        local_filename = mssg.origin_filename
        sdfs_filename = mssg.dest_filename
        local_filesize = os.path.getsize(local_filename)

        # Create a socket connection with each machine in replica list and write the replicas to disk
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        for node_id in replica_list:
            host, port, version = node_id
            sock_fd.connect((host, port))
            sock_fd.send(f"{local_filename}{SEPARATOR}{local_filesize}{SEPARATOR}{sdfs_filename}".encode())

            progress = tqdm.tqdm(range(local_filesize), f"Sending {local_filename}", unit="B", unit_scale=True, unit_divisor=1024)
            with open(local_filename, "rb") as f:
                while True:
                    # read the bytes from the file
                    bytes_read = f.read(BUFFER_SIZE)
                    if not bytes_read:
                        # file transmitting is done
                        break
                    # we use sendall to assure transimission in busy networks
                    sock_fd.sendall(bytes_read)
                    progress.update(len(bytes_read))

            sock_fd.close()

    def receive_writes():
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        sock_fd.bind() # TODO: bind to any incoming message on this socket

        while True:

            # accept connection if there is any
            client_socket, address = sock_fd.accept()
            # if below code is executed, that means the sender is connected
            print(f"[+] {address} is connected.")

            received = client_socket.recv(BUFFER_SIZE).decode()
            local_filename, filesize, sdfs_filename = received.split(SEPARATOR)
            # filename = os.path.basename(filename)
            # convert to integer
            filesize = int(filesize)
            progress = tqdm.tqdm(range(filesize), f"Receiving {local_filename}", unit="B", unit_scale=True, unit_divisor=1024)

            with open(sdfs_filename, "wb") as f:
                while True:
                    # read 1024 bytes from the socket (receive)
                    bytes_read = client_socket.recv(BUFFER_SIZE)
                    if not bytes_read:    
                        # nothing is received
                        # file transmitting is done
                        break
                    # write to the file the bytes we just received
                    f.write(bytes_read)
                    progress.update(len(bytes_read))

            # close the client socket
            client_socket.close()
            # close the server socket
            sock_fd.close()

    def leader_work(self):
        """The leader should process messages about reads/writes/deletes"""
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind() # TODO: bind to any incoming message on this socket

        while True:
            if self.nodeId == self.leader_node:
                data, server = sock_fd.recvfrom(MAX) # receive serialized data from another machine
            
                if self.status == 'Joined':
                    if data:
                        mssg = pickle.loads(data)

                local_filename = mssg.origin_filename
                sdfs_filename = mssg.dest_filename

                if sdfs_filename not in self.membership_list.file_replication_dict:
                    # TODO: Create a new file

                    servers = self.membership_list.active_nodes.keys()
                    replica_servers = random.sample(servers, REPLICATION_FACTOR)
                    replica_dict = {'primary': replica_servers[0], 'secondary': replica_servers[1:]}

                    # TODO: Send the file to the appropriate replica servers
                else:
                    # TODO: Update the file
                    # TODO: Updating the file replication dict should be done after receiving ACK from the client?
                    replica_dict = self.membership_list.file_replication_dict[sdfs_filename]
                    replica_servers = [replica_dict['primary']] + replica_dict['secondary']

    def start_machine(self):
        ''' Start the server '''
        leader_election_thread = threading.Thread(target=self.leader_election)
        server_read_write_thread = threading.Thread(target=self.server_read_write)
        receive_writes_thread = threading.Thread(target=self.receive_writes)
        leader_thread = threading.Thread(target=self.leader_work)

        leader_election_thread.start()
        leader_thread.start()
        server_read_write_thread.start()
        receive_writes_thread.start()
        # leader_election_thread.join()
    


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()