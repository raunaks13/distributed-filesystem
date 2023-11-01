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
from collections import defaultdict
import copy

# Static variables
BASE_PORT = 8000
BASE_FS_PORT = 9000
BASE_WRITE_PORT = 10000
BASE_READ_PORT = 11000
BASE_DELETE_PORT = 12000
BASE_FS_PING_PORT = 13000
BASE_REREPLICATION_PORT = 14000
REPLICATION_FACTOR = 2
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
        self.delete_port = BASE_DELETE_PORT + MACHINE_NUM
        self.fs_ping_port = BASE_FS_PING_PORT + MACHINE_NUM
        self.rereplication_port = BASE_REREPLICATION_PORT + MACHINE_NUM
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


    def store(self, directory):
        for filename in os.listdir(directory):
            path = os.path.join(directory, filename)
            if os.path.isfile(path):
                size = os.path.getsize(path)
                print(f"File: {filename}, Size: {size} bytes")


    def list_replica_dict(self):
        print(self.machine.membership_list.file_replication_dict.items())
        print("\n")


    def list_failed_nodes(self):
        print(self.machine.membership_list.failed_nodes)
        print("\n")

    
    def ls_sdfsfilename(self, sdfsfilename):
        print(self.machine.membership_list.file_replication_dict[sdfsfilename])
        print("\n")


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
            if self.machine.status == "Joined" and self.leader_node is not None:
                if self.machine.nodeId[3] == self.leader_node[3]:
                    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock_fd.bind((self.ip, self.fs_ping_port))
                    sock_fd.listen(5)

                    while self.machine.nodeId[3] == self.leader_node[3]:
                        self.machine.logger.debug(f"File Replication Dict: {self.machine.membership_list.file_replication_dict}")

                        conn, addr = sock_fd.accept()
                        try:
                            data = conn.recv(MAX)   # receive serialized data from another machine
                            mssg = pickle.loads(data)

                            if mssg.type == 'write_ping':
                                file = mssg.kwargs['replica']
                                if file not in self.machine.membership_list.file_replication_dict:
                                    self.machine.membership_list.file_replication_dict[file] = {mssg.host}
                                else:
                                    self.machine.membership_list.file_replication_dict[file].add(mssg.host)
                            
                            elif mssg.type == 'delete_ping':
                                file = mssg.kwargs['replica']
                                if file in self.machine.membership_list.file_replication_dict:
                                    self.machine.membership_list.file_replication_dict[file].remove(mssg.host)

                        except Exception as e:
                            self.machine.logger.error(f"Error in receiving filestore ping: {e}")

                        finally:
                            conn.close()
                    sock_fd.close()

    

    def filestore_ping(self, op_type, replica):
        ''' Send a ping message to leader about the replicas stored '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((self.leader_node[0], BASE_FS_PING_PORT  + self.leader_node[3])) 

        if op_type == 'w':
            msg_type = 'write_ping'
        elif op_type == 'r':
            msg_type = 'read_ping'
        elif op_type == 'd':
            msg_type = 'delete_ping'
        else:
            msg_type = None

        msg = Message(msg_type=msg_type, 
                    host=self.machine.nodeId, 
                    membership_list=None, 
                    counter=None,
                    replica=replica
                    )
        self.send_message(sock_fd, pickle.dumps(msg))
        sock_fd.close()



    def rereplication_leader(self):
        ''' Re-replicate the files stored in the failed node '''
        while True:
            if (self.machine.status == "Joined") and \
                (self.leader_node is not None): # leader must be a part of file replication dict
                if self.machine.nodeId[3] == self.leader_node[3]:
                    if len(self.machine.membership_list.failed_nodes) > 0:
                        node = self.machine.membership_list.failed_nodes[0]

                        d = self.machine.membership_list.file_replication_dict
                        inverted_replica_dict = defaultdict(list) # dict from machine tuple : list of filenames
                        for fil, v in d.items():
                            for m in v:
                                inverted_replica_dict[m].append(fil)


                        replica_rereplication = inverted_replica_dict[node] # filenames in failed node
                        # print("Inverted replica dict", inverted_replica_dict.items())

                        for filename in replica_rereplication: 
                            alive_replica_node = None
                            new_replica_node = None
                            # Find the new node where the replica will be stored
                            while True:
                                new_replica_node = random.sample(self.machine.membership_list.active_nodes.keys(), 1)[0]
                                replica_nodes = self.machine.membership_list.file_replication_dict[filename]
                                if new_replica_node not in replica_nodes:
                                    break

                            # Find a node from where the content will be copied to the new node
                            all_replica_nodes = copy.deepcopy(self.machine.membership_list.file_replication_dict[filename])
                            all_replica_nodes.remove(node)
                            alive_replica_node = list(all_replica_nodes)[0]
                            
                            
                            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            sock_fd.connect((alive_replica_node[0], BASE_REREPLICATION_PORT  + alive_replica_node[3]))

                            mssg = Message(msg_type='put_replica',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            filename=filename,
                                            replica_node=new_replica_node
                                            )
                            print("Sending message to {}: Instr copy {} from {} to {} ".format(alive_replica_node, filename, alive_replica_node, new_replica_node))
                            self.send_message(sock_fd, pickle.dumps(mssg))
                            # Wait for ACK
                            data = sock_fd.recv(MAX)
                            mssg = pickle.loads(data)
                            sock_fd.close()

                            if mssg.type == "ACK":
                                self.machine.membership_list.file_replication_dict[filename].add(new_replica_node)
                                self.machine.membership_list.failed_nodes.pop(0)
                                self.machine.membership_list.file_replication_dict[filename].remove(node)
                            else:
                                # TODO: What if rereplication failed, what to do?
                                pass



    def write_replicas(self, filename, replica):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((replica[0], replica[3] + BASE_WRITE_PORT)) 
        print(replica[0], replica[3]+BASE_WRITE_PORT)

        # Send filename message to the replicas
        self.send_message(sock_fd, pickle.dumps(filename))
        data = sock_fd.recv(MAX)
        # If file is opened, send the file content
        if "ACK" == pickle.loads(data):

            with open(os.path.join(HOME_DIR, filename), 'rb') as f:
                bytes_read = f.read()
                # if not bytes_read:
                    # break

                while bytes_read:
                    self.send_message(sock_fd, bytes_read)
                    bytes_read = f.read()
            
            sock_fd.shutdown(socket.SHUT_WR)
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            if mssg.type == "ACK":
                return 1

        return 0


    def rereplication_follower(self):
        ''' Receive the message from leader to re-replicate the files stored in the failed node '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.rereplication_port))
        sock_fd.listen(5)

        while True:
            if self.machine.status == "Joined"  and self.leader_node is not None:
                # if self.machine.nodeId[3] != self.leader_node[3]:
                conn, addr = sock_fd.accept()
                try:
                    data = conn.recv(MAX)   # receive serialized data from another machine
                    mssg = pickle.loads(data)

                    if mssg.type == 'put_replica':
                        filename = mssg.kwargs['filename']
                        replica_node = mssg.kwargs['replica_node']
                        ret = self.write_replicas(filename, replica_node)
                        print("Ret:", ret)
                        print(filename, replica_node)
                        if ret == 1:
                            mssg = Message(msg_type='ACK',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                        else:
                            mssg = Message(msg_type='NACK',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                finally:
                    conn.close()



    def receive_writes(self):
        # TODO: Lock a file
        # TODO:How to enable multiple writes to different files if the conn is established
        
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.write_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            print(conn, addr)
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

                self.filestore_ping('w', filename)
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


    def receive_deletes(self):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.delete_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            print(conn, addr)
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                print(f"Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                os.remove(os.path.join(HOME_DIR, filename))

                self.filestore_ping('d', filename)
                mssg = Message(msg_type='ACK',
                                host=self.machine.nodeId,
                                membership_list=None,
                                counter=None,
                              )
                self.send_message(conn, pickle.dumps(mssg))
                
            finally:
                conn.close()


 
    def leader_work(self, mssg_type, sdfs_filename):
        ''' Leader should process messages about reads/writes/deletes '''

        if mssg_type == 'put':
            # For Write messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                # Choose Replication Servers if file is present
                servers = self.machine.membership_list.active_nodes.keys()
                replica_servers = random.sample(servers, REPLICATION_FACTOR)
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]
                print("Leader computing replica servers 1...", replica_servers)
                # ret = self.write_replicas(sdfs_filename, content, replica_servers)
                # if ret == 1:
                #     self.machine.membership_list.file_replication_dict[sdfs_filename] = replica_servers

            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]
                print("Leader computing replica servers 2...", replica_servers)

                # ret = self.write_replicas(sdfs_filename, content, replica_servers)
                

        elif mssg_type == 'get':
            # For Read messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                # Use the replication servers from the membership list
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_READ_PORT, server[2], server[3]) for server in replica_servers]

                # ret = self.read_replica(sdfs_filename, replica_servers[0:READ_QUORUM])
        

        elif mssg_type == 'delete':
            # For Delete messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_DELETE_PORT, server[2], server[3]) for server in replica_servers]

                # ret = self.delete_replica(sdfs_filename, replica_servers[0:WRITE_QUORUM])

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

                if self.machine.nodeId[3] != self.leader_node[3]:
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
                            print("Put message received at leader")
                            replicas = self.leader_work(mssg.type, mssg.kwargs['filename'])
                            mssg = Message(msg_type='replica',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            replica=replicas
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                            print("Message regarding replicas sent to client")

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
                                                replicas=replicas
                                                )
                                self.send_message(conn, pickle.dumps(mssg))

                    elif mssg.type == 'delete':
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
                                                replica=replicas
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
        receive_deletes_thread = threading.Thread(target=self.receive_deletes)
        filestore_ping_recv_thread = threading.Thread(target=self.filestore_ping_recv)
        rereplication_leader_thread = threading.Thread(target=self.rereplication_leader)
        rereplication_follower_thread = threading.Thread(target=self.rereplication_follower)

        leader_election_thread.start()
        receive_thread.start()
        receive_writes_thread.start()
        receive_reads_thread.start()
        receive_deletes_thread.start()
        filestore_ping_recv_thread.start()
        rereplication_leader_thread.start()
        rereplication_follower_thread.start()
    


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()