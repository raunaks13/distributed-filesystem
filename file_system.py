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
import shutil
import numpy as np

# Static variables
BASE_PORT = 8000
BASE_FS_PORT = 9000
BASE_WRITE_PORT = 10000
BASE_READ_PORT = 11000
BASE_FS_PING_PORT = 12000
REPLICATION_FACTOR = 1
WRITE_QUORUM = 3
READ_QUORUM = 1
MAX = 8192

BUFFER_SIZE = 4096
SEPARATOR = " "
HOME_DIR = "./DS"


class File_System:

    def __init__(self, MACHINE_NUM, MACHINE):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_FS_PORT + MACHINE_NUM
        self.write_port = BASE_WRITE_PORT + MACHINE_NUM
        self.read_port = BASE_READ_PORT + MACHINE_NUM
        self.fs_ping_port = BASE_FS_PING_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.machine = MACHINE

        self.leader_node = None

        shutil.rmtree(HOME_DIR)
        os.mkdir(HOME_DIR)


    def send_message(self, sock_fd, msg):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendall(msg)
        except:
            pass


    def leader_election(self, sock_fd=None):
        ''' Detect whether the leader has failed 
            If the leader has failed, start a new election
        '''
        while True:
            if self.machine.status == "Joined":
                # if self.leader_node == None or self.leader_node not in self.machine.membership_list.active_nodes: # condition for running leader election
                machines = list(self.machine.membership_list.active_nodes.keys())
                machine_ids = [machine[1] for machine in machines]
                max_machine_id_index = np.argmax(machine_ids)

                leader_node = machines[max_machine_id_index]
                self.leader_node = (leader_node[0], leader_node[1] - BASE_PORT + BASE_FS_PORT, leader_node[2], leader_node[3])
                self.machine.logger.info(f"New leader elected: {self.leader_node}")

                # TODO: Send a message to all machines to update the leader
                # host = (self.ip, self.port, self.version)
                # msg = Message(msg_type='leader_update', 
                #               host=host, 
                #               membership_list=self.machine.membership_list, 
                #               counter=self.ping_counter
                #              )
                # self.send_leader_msg(msg, sock_fd)

                # TODO: For the failed node, check the replicas stored and re-replicate the files
                time.sleep(4)


    def filestore_ping_recv(self):
        ''' Receive a ping message at leader and update the file replication list '''
        while True:
            if self.machine.status == "Joined":
                if self.machine.nodeId[3] == self.leader_node[3]:
                    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock_fd.bind((self.ip, self.fs_ping_port))
                    sock_fd.listen(5)

                    while self.machine.nodeId[3] == self.leader_node[3]:
                        print('File Replication Dict:', self.machine.membership_list.file_replication_dict)

                        conn, addr = sock_fd.accept()
                        try:
                            data = conn.recv(MAX)   # receive serialized data from another machine
                            mssg = pickle.loads(data)

                            if mssg.type == 'ping':
                                file_list = mssg.kwargs['replica_list']
                                for file in file_list:
                                    if file not in self.machine.membership_list.file_replication_dict:
                                        self.machine.membership_list.file_replication_dict[file] = {mssg.host}
                                    else:
                                        self.machine.membership_list.file_replication_dict[file].add(mssg.host)
                        except Exception as e:
                            print(e)
                            pass
                        finally:
                            conn.close()
                    sock_fd.close()


    def filestore_ping(self):
        ''' Send a ping message to leader about the replicas stored '''
        while True:
            if self.machine.status == "Joined":
                try:
                    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock_fd.connect((self.leader_node[0], BASE_FS_PING_PORT  + self.leader_node[3])) 

                    replica_list = []
                    for file in os.listdir(HOME_DIR):
                        replica_list.append(file)

                    msg = Message(msg_type='ping', 
                                host=self.machine.nodeId, 
                                membership_list=None, 
                                counter=None,
                                replica_list=replica_list
                                )
                    self.send_message(sock_fd, pickle.dumps(msg))
                    sock_fd.close()
                except Exception as e:
                    print(e)
                    pass

            time.sleep(2)


    def receive_writes(self):
        # TODO: Lock a file
        # TODO:How to enable multiple writes to different files if the conn is established
        
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.write_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                print(f"Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                f = open(os.path.join(HOME_DIR, filename), 'wb')
                if f:
                    conn.sendall(pickle.dumps("ACK"))

                # Receive file content, write to the file and send ACK
                print("Receiving file content")
                
                bytes_read = conn.recv(BUFFER_SIZE)
                while bytes_read:
                    if not bytes_read:
                        break
                    else:
                        # write to the file the bytes we just received
                        f.write(bytes_read)
                        bytes_read = conn.recv(BUFFER_SIZE)

                print("File received")
                f.close()

                mssg = Message(msg_type='ACK',
                                host=self.machine.nodeId,
                                membership_list=None,
                                counter=None,
                              )
                self.send_message(conn, pickle.dumps(mssg))

            finally:
                conn.close()


    """
    def write_replicas(self, sdfs_filename, content, replica_servers):
        ''' Send the file name and the file content to the replica servers '''
        # TODO: Parallelize this code

        ack_count = 0
        for replica in replica_servers:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1]))

            # Send filename message to the replicas
            self.send_message(sock_fd, pickle.dumps(sdfs_filename))
            data = sock_fd.recv(MAX)
            print(pickle.loads(data))
            # If file is opened, send the file content
            if "ACK" == pickle.loads(data):
                sock_fd.sendall( pickle.dumps(content) )
                data = sock_fd.recv(MAX)
                if "ACK" == pickle.loads(data):
                    ack_count += 1

            sock_fd.close()

        if ack_count >= WRITE_QUORUM:
            return 1
        
        return 0
    """


    def receive_reads(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.read_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                print(f"Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                with open(os.path.join(HOME_DIR, filename), 'rb') as f:
                    bytes_read = f.read()
                    if not bytes_read:
                        break

                    while bytes_read:
                        self.send_message(conn, bytes_read)
                        bytes_read = f.read()
                
                conn.shutdown(socket.SHUT_WR)
                
            finally:
                conn.close()
        

    """
    def read_replica(self, sdfs_filename, replica_servers):
        ack_count == 0
        file_content = None

        for replica in replica_servers:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1]))

            # Send filename message to the replicas
            self.send_message(sock_fd, pickle.dumps(sdfs_filename))

            # Receive the file contents
            data = None
            while True:
                data = sock_fd.recv(MAX)
                if not data:
                    break

            content = pickle.loads(data)
            if content != "NACK":
                ack_count += 1
            else:
                file_content = content

        if ack_count >= READ_QUORUM:
            return file_content

        return 0
    """

 
    def leader_work(self, mssg_type, sdfs_filename):
        ''' Leader should process messages about reads/writes/deletes '''

        if mssg_type == 'put':
            # For Write messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                # Choose Replication Servers if file is present
                servers = self.machine.membership_list.active_nodes.keys()
                replica_servers = random.sample(servers, REPLICATION_FACTOR)
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]

                # ret = self.write_replicas(sdfs_filename, content, replica_servers)
                # if ret == 1:
                #     self.machine.membership_list.file_replication_dict[sdfs_filename] = replica_servers

            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]
                
                # ret = self.write_replicas(sdfs_filename, content, replica_servers)
                

        elif mssg_type == 'get':
            # For Read messaged
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                # Use the replication servers from the membership list
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_READ_PORT, server[2], server[3]) for server in replica_servers]

                # ret = self.read_replica(sdfs_filename, replica_servers[0:READ_QUORUM])

        return replica_servers


    def receive(self):
        ''' Receive messages and act accordingly (get/put/delete) '''
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        print("Receiving messages from leader on port ", self.port)
        recv_sock_fd.bind((self.ip, self.port))
        recv_sock_fd.listen(5)
        print("Listening on port ", self.ip, self.port)

        while True:
            conn, addr = recv_sock_fd.accept()

            try:
                data = conn.recv(MAX) # receive serialized data from another machine
                mssg = pickle.loads(data)
                print(mssg.type, self.machine.nodeId)

                if self.machine.nodeId[3] == self.leader_node[3]:
                    # If not leader, ask client to send to leader
                    print("Message received at non-leader")
                    mssg = Message(msg_type='leader',
                                    host=self.machine.nodeId,
                                    membership_list=None,
                                    counter=None,
                                    leader=self.leader_node
                                    )
                    self.send_message(conn, pickle.dumps(mssg))  

                else:
                    if mssg.type == 'put':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(mssg.type, mssg.kwargs['filename'])
                            mssg = Message(msg_type='replica',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            replica=replicas
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                            print("Message regarding replcias sent to client")

                    elif mssg.type == 'get':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(mssg.type, mssg.kwargs['filename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                mssg = Message(msg_type='replica',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=replicas[0]
                                                )
                                self.send_message(conn, pickle.dumps(mssg))

            finally:
                conn.close()


    def start_machine(self):
        ''' Start the server '''
        leader_election_thread = threading.Thread(target=self.leader_election)
        receive_thread = threading.Thread(target=self.receive)
        receive_writes_thread = threading.Thread(target=self.receive_writes)
        receive_reads_thread = threading.Thread(target=self.receive_reads)
        filestore_ping_thread = threading.Thread(target=self.filestore_ping)
        filestore_ping_recv_thread = threading.Thread(target=self.filestore_ping_recv)

        leader_election_thread.start()
        receive_thread.start()
        receive_writes_thread.start()
        receive_reads_thread.start()
        filestore_ping_thread.start()
        filestore_ping_recv_thread.start()
    


if __name__ == "__main__":
    MACHINE_NUM = sys.argv[1]

    machine = Machine(int(MACHINE_NUM))
    machine.start_machine()