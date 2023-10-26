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
BASE_PORT = 8000
BASE_FS_PORT = 9000
BASE_DATA_PORT = 10000
REPLICATION_FACTOR = 2
WRITE_QUORUM = 3
READ_QUORUM = 1
MAX = 8192

BUFFER_SIZE = 4096
SEPARATOR = " "


class File_System:

    def __init__(self, MACHINE_NUM, LOGGER, MEM_LIST, STATUS):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_FS_PORT + MACHINE_NUM
        self.data_port = BASE_DATA_PORT + MACHINE_NUM
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


    def send_message(self, sock_fd, msg):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendall(msg)
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

                # TODO: For the failed node, check the replicas stored and re-replicate the files
                time.sleep(8)


    def get(self, mssg, sock_fd):
        ''' Read a file from the SDFS '''
        if self.leader_node == self.nodeId:
            # If leader
            sdfsfilename = mssg.sdfsfilename

            if sdfsfilename not in self.membership_list.file_replication_dict:
                # No file found error
                msg = Message('Error', self.nodeId, None, None, error_message="File Does not Exist")
                self.send_message(sock_fd, pickle.dumps(msg))
            else:
                replica_dict = self.membership_list.file_replication_dict[sdfsfilename]
                replica_server = replica_dict['primary']  # Because READ_QUORUM = 1

                # TODO: Get from the appropriate replica server

        else:
            # If not leader, forward the message to the current leader
            self.send_message(sock_fd, data)



    def delete(self, mssg, sock_fd):
        ''' Delete a file from the SDFS '''
        if self.leader_node == self.nodeId:
            # If leader
            sdfsfilename = mssg.sdfsfilename

            if sdfsfilename not in self.membership_list.file_replication_dict:
                # No file found error
                msg = Message('Error', self.nodeId, None, None, error_message="File Does not Exist")
                self.send_message(sock_fd, pickle.dumps(msg)])
            else:
                replica_dict = self.membership_list.file_replication_dict[sdfsfilename]
                replica_server = replica_dict['primary']

                # TODO: Delete from all the appropriate replica server

        else:
            # If not leader, forward the message to the current leader
            self.send_message(sock_fd, data])


    def receive_writes(self):
        # TODO: Lock for a file
        # TODO:How to enable multiple writes to different files if the conn is established
        
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.data_port))
        sock_fd.listen(5)

        while True:
            conn, addr = recv_sock_fd.accept()
            file = None

            try:
                # Receive Filename and open the file. If file exists, ACK
                while True:
                    data = conn.recv(MAX)
                    if not data:
                        break
                    filename = pickle.loads(data)
                    file = open(filename, 'a')

                if file:
                    conn.sendall(pickle.dumps("ACK"))

                # Receive file content, write to the file and send ACK
                while True:
                    data = conn.recv(MAX)
                    if not data:
                        break
                    file.write(data)

                conn.sendall(pickle.dumps("ACK"))

            finally:
                conn.close()


    def receive_reads(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.data_port))
        sock_fd.listen(5)



    def write_replicas(self, sdfs_filename, content, replica_servers):
        ''' Send the file name and the file content to the replica servers '''
        # TODO: Parallelize this code

        ack_count == 0
        for replica in replica_server:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1]))

            # Send filename message to the replicas
            self.send_message(sock_fd, pickle.dumps(sdfs_filename))
            data = sock.recv(MAX)
            # If file is opened, send the file content
            if pickle.loads(data).contains("ACK"):
                sock_fd.sendall(content)
                data = sock.recv(MAX)
                if pickle.loads(data).contains("ACK"):
                    ack_count += 1

        if ack_count >= WRITE_QUORUM:
            return 1
        
        return 0


    def read_replica(self, sdfs_filename, replica_servers):
        for replica in replica_server:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1]))

 
    def leader_work(self, mssg_type, sdfs_filename, content):
        ''' Leader should process messages about reads/writes/deletes '''
        if sdfs_filename not in self.membership_list.file_replication_dict:
            # Choose Replication Servers if file is present
            servers = self.membership_list.active_nodes.keys()
            replica_servers = random.sample(servers, REPLICATION_FACTOR)
            replica_servers = [(server[0], server[1] - BASE_PORT + BASE_DATA_PORT, server[2]) for server in replica_servers]

            if mssg_type == 'put':
                ret = self.write_replicas(sdfs_filename, content, replica_servers)
                if ret == 1:
                    self.membership_list.file_replication_dict[sdfs_filename] = replica_servers

            elif mssg_type == 'get':
                ret = 0
        else:
            # Use the replication servers from the membership list
            replica_servers = self.membership_list.file_replication_dict[sdfs_filename]
            replica_servers = [(server[0], server[1] - BASE_PORT + BASE_DATA_PORT, server[2]) for server in replica_servers]

            if mssg_type == 'put':
                ret = self.write_replicas(sdfs_filename, content, replica_servers)

            elif mssg_type == 'get':
                ret = self.read_replica(sdfs_filename, replica_server[0:READ_QUORUM])

        return ret


    def receive(self):
        ''' Receive messages and act accordingly (get/put/delete) '''
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        recv_sock_fd.bind((self.ip, self.port))
        recv_sock_fd.listen(5)

        while True:
            conn, addr = recv_sock_fd.accept()

            try:
                while True:
                    data = conn.recv(MAX) # receive serialized data from another machine
                    if not data:
                        break

                    mssg = pickle.loads(data)

                    if mssg.type == 'put':
                        if self.nodeId == self.leader_node:
                            ret = self.leader_work(mssg_type, mssg.filename, msg.content)
                            if ret == 1:
                                self.send_message(conn, pickle.dumps("ACK"))
                            else:
                                self.send_message(conn, pickle.dumps("NACK"))
                            
                            # TODO: Implement FTP
                            # Send ACK to Client that the file will be written and open a new connection in a forked child to receive the file content
                            # Get the file from the client, send it to all the replica servers and then send an ack to the client
                        else:
                            # If not leader, forward the message to the current leader
                            send_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            send_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            try:
                                send_sock_fd.connect((self.leader_node[0], self.leader_node[1]))
                                self.send_message(send_sock_fd, data)
                            except:
                                print("Send not possible from Node: ", self.nodeId)
                            send_sock_fd.close()

                    elif mssg.type == 'get':
                        if self.nodeId == self.leader_node:
                            ret = self.leader_work(mssg_type, mssg.filename, None)
                            if ret == 0:
                                self.send_message(conn, pickle.dumps("NACK"))
                            else:
                                conn.sendfile(ret)
                        else:
                            # If not leader, forward the message to the current leader
                            send_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            send_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            try:
                                send_sock_fd.connect((self.leader_node[0], self.leader_node[1]))
                                self.send_message(send_sock_fd, data)
                            except:
                                print("Send not possible from Node: ", self.nodeId)
                            send_sock_fd.close()

            finally:
                conn.close()

    '''
    def server_read_write(self):
        """receive messages and act accordingly (get/put/delete)"""
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCP
        

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
    '''

    def start_machine(self):
        ''' Start the server '''
        leader_election_thread = threading.Thread(target=self.leader_election)
        receive_thread = threading.Thread(target=self.receive)
        receive_writes_thread = threading.Thread(target=self.receive_writes)
        receive_reads_thread = threading.Thread(target=self.receive_reads)

        leader_election_thread.start()
        receive_thread.start()
        receive_writes_thread.start()
        receive_reads_thread.start()
        # receive_thread.join()
        # leader_election_thread.join()
    


if __name__ == "__main__":
    MACHINE_NUM = sys.argv[1]

    machine = Machine(int(MACHINE_NUM))
    machine.start_machine()