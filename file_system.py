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
# import numpy as np
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
REPLICATION_FACTOR = 3
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

        try:
            shutil.rmtree(HOME_DIR)
        except:
            pass
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
        print("\n")


    def list_replica_dict(self):
        print(self.machine.membership_list.file_replication_dict.items())
        print("\n")


    def list_failed_nodes(self):
        print(self.machine.membership_list.failed_nodes)
        print("\n")


    def ls_sdfsfilename(self, sdfsfilename):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((self.leader_node[0], BASE_FS_PORT  + self.leader_node[3]))

        mssg = Message(msg_type='ls',
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        sdfsfilename=sdfsfilename
                        )
        self.send_message(sock_fd, pickle.dumps(mssg))
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "replica":
            print("------Replicas:------")
            for replica in mssg.kwargs['replica']:
                print(f"Machine Num: {replica[3]}, IP: {replica[0]}")
        else:
            print("File not found")
        print("\n")


    def get_leader_node(self):
        return self.leader_node


    def argmax(self, a):
        return max(range(len(a)), key=lambda x: a[x])

    def leader_election(self, sock_fd=None):
        ''' Detect whether the leader has failed 
            If the leader has failed, start a new election
        '''
        while True:
            if self.machine.status == "Joined":
                # if self.leader_node == None or self.leader_node not in self.machine.membership_list.active_nodes: # condition for running leader election
                machines = list(self.machine.membership_list.active_nodes.keys())
                machine_ids = [machine[1] for machine in machines]
                # max_machine_id_index = np.argmax(machine_ids)
                max_machine_id_index = self.argmax(machine_ids)

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
                                
                                if len(self.machine.membership_list.file_replication_dict[file]) == REPLICATION_FACTOR: # should be min(num_machines, replication factor)
                                    self.machine.membership_list.file_lock_set.remove(file)
                            
                            elif mssg.type == 'delete_ping':
                                file = mssg.kwargs['replica']
                                if file in self.machine.membership_list.file_replication_dict:
                                    self.machine.membership_list.file_replication_dict[file].remove(mssg.host)
                                
                                # if len(self.machine.membership_list.file_replication_dict[file]) == 0: # TODO: should the key even exist?
                                #     self.machine.membership_list.file_lock_set.remove(file)

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
                            self.machine.logger.info("[Re-Replication] Sending message to {}: Instr copy replica {} from {} to {} ".format(alive_replica_node, filename, alive_replica_node, new_replica_node))
                            self.send_message(sock_fd, pickle.dumps(mssg))
                            # Wait for ACK
                            data = sock_fd.recv(MAX)
                            mssg = pickle.loads(data)
                            sock_fd.close()

                            if mssg.type == "ACK":
                                self.machine.membership_list.file_replication_dict[filename].add(new_replica_node)
                                self.machine.membership_list.file_replication_dict[filename].remove(node)
                            else:
                                # TODO: What if rereplication failed, what to do?
                                pass

                        self.machine.membership_list.failed_nodes.pop(0)



    def write_replicas(self, filename, replica):
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((replica[0], replica[3] + BASE_WRITE_PORT)) 

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
                        self.machine.logger.info(f"[Re-Replication] Putting Replica of {filename} in {replica_node}")
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
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.bind((self.ip, self.write_port))
        sock_fd.listen(5)

        while True:
            conn, addr = sock_fd.accept()
            try:
                # Receive Filename and open the file. If file exists, ACK
                data = conn.recv(MAX)
                self.machine.logger.info(f"[Write] Filename received: {pickle.loads(data)}")
                filename = pickle.loads(data)

                f = open(os.path.join(HOME_DIR, filename), 'wb')
                if f:
                    conn.sendall(pickle.dumps("ACK"))

                # Receive file content, write to the file and send ACK
                self.machine.logger.info("[Write] Receiving file content...")
                
                bytes_read = conn.recv(BUFFER_SIZE)
                while bytes_read:
                    if not bytes_read:
                        break
                    else:
                        # write to the file the bytes we just received
                        f.write(bytes_read)
                        bytes_read = conn.recv(BUFFER_SIZE)

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
                self.machine.logger.info(f"[Read] Filename received: {pickle.loads(data)}")
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
                self.machine.logger.info(f"[Delete] Filename received: {pickle.loads(data)}")
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


    def read_replicas(self, sdfs_filename, local_filename, replicas):
        for replica in replicas:
            try:
                sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock_fd.connect((replica[0], replica[1]))
                break 
            except:
                continue

        # Send filename message to the replicas
        self.send_message(sock_fd, pickle.dumps(sdfs_filename))

        with open(local_filename, 'wb') as f:
            bytes_read = sock_fd.recv(BUFFER_SIZE)
            while bytes_read:
                if not bytes_read:
                    break
                else:
                    # write to the file the bytes we just received
                    f.write(bytes_read)
                    bytes_read = sock_fd.recv(BUFFER_SIZE)
        
        sock_fd.close()
        print("[ACK Received] Get file successfully\n")


    def leader_work(self, mssg_type, sdfs_filename):
        ''' Leader should process messages about reads/writes/deletes '''

        if mssg_type == 'put':
            # For Write messages       
            while True:
                if sdfs_filename not in self.machine.membership_list.file_lock_set:
                    self.machine.membership_list.file_lock_set.add(sdfs_filename)
                    print(f"File Lock Set: {self.machine.membership_list.file_lock_set}")

                    if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                        # Choose Replication Servers if file is present
                        servers = self.machine.membership_list.active_nodes.keys()
                        replica_servers = random.sample(servers, REPLICATION_FACTOR)
                        replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]
                    else:
                        replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                        print(f"Case 2 Replica Servers: {replica_servers}")
                        replica_servers = [(server[0], server[1] - BASE_PORT + BASE_WRITE_PORT, server[2], server[3]) for server in replica_servers]

                    break           

        elif (mssg_type == 'get' or mssg_type == 'multiread'):
            # For Read messages
                
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                # Use the replication servers from the membership list
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_READ_PORT, server[2], server[3]) for server in replica_servers]
                        
        elif mssg_type == 'delete':

            # For Delete messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
                replica_servers = [(server[0], server[1] - BASE_PORT + BASE_DELETE_PORT, server[2], server[3]) for server in replica_servers]

        elif mssg_type == 'ls':
            # For ls messages
            if sdfs_filename not in self.machine.membership_list.file_replication_dict:
                replica_servers = []
            else:
                replica_servers = self.machine.membership_list.file_replication_dict[sdfs_filename]
        
        return replica_servers


    def receive(self):
        ''' Receive messages and act accordingly (get/put/delete) '''
        recv_sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recv_sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        recv_sock_fd.bind((self.ip, self.port))
        recv_sock_fd.listen(5)

        while True:
            conn, addr = recv_sock_fd.accept()

            try:
                data = conn.recv(MAX) # receive serialized data from another machine
                recv_mssg = pickle.loads(data)

                if recv_mssg.type == 'multiread_get':
                    if os.fork() == 0:
                        self.read_replicas(recv_mssg.kwargs['sdfs_filename'], recv_mssg.kwargs['local_filename'], recv_mssg.kwargs['replica'])
                        

                if self.machine.nodeId[3] != self.leader_node[3]:
                    # If not leader, ask client to send to leader
                    mssg = Message(msg_type='leader',
                                    host=self.machine.nodeId,
                                    membership_list=None,
                                    counter=None,
                                    leader=self.leader_node
                                    )
                    self.send_message(conn, pickle.dumps(mssg))  

                else:
                    if recv_mssg.type == 'put':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            self.machine.logger.info("[Receive at Leader] Put message received at leader")
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
                            mssg = Message(msg_type='replica',
                                            host=self.machine.nodeId,
                                            membership_list=None,
                                            counter=None,
                                            replica=replicas
                                            )
                            self.send_message(conn, pickle.dumps(mssg))
                            self.machine.logger.info("Message regarding replicas sent to client")

                    elif recv_mssg.type == 'get':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
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

                    elif recv_mssg.type == 'multiread':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['sdfs_filename'])
                            if len(replicas) == 0:
                                mssg = Message(msg_type='NACK',
                                                host=self.machine.nodeId,
                                                membership_list=None,
                                                counter=None,
                                                replica=None
                                                )
                                self.send_message(conn, pickle.dumps(mssg))
                            else:
                                # For each VM in the multiread group, send the replica list
                                for machine in recv_mssg.kwargs['machines']:
                                    machine_num = int(machine[2:])
                                    
                                    if machine_num == self.machine.nodeId[3]:
                                        mssg = Message(msg_type='replica',
                                                        host=self.machine.nodeId,
                                                        membership_list=None,
                                                        counter=None,
                                                        replica=replicas
                                                        )
                                        self.send_message(conn, pickle.dumps(mssg))

                                    all_nodes = list(self.machine.membership_list.active_nodes.keys())
                                    # Find the VM IP and Port where multiread need to initiated
                                    send_node = None
                                    for node in all_nodes:
                                        if node[3] == machine_num:
                                            send_node = node
                                            break
                                    
                                    sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                    sock_fd.connect((send_node[0], BASE_FS_PORT  + send_node[3]))

                                    mssg = Message(msg_type='multiread_get',
                                                    host=self.machine.nodeId,
                                                    membership_list=None,
                                                    counter=None,
                                                    sdfs_filename=recv_mssg.kwargs['sdfs_filename'],
                                                    local_filename=recv_mssg.kwargs['local_filename'],
                                                    replica=replicas
                                                    )
                                    self.send_message(sock_fd, pickle.dumps(mssg))
                                    sock_fd.close()

                    elif recv_mssg.type == 'delete':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['filename'])
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

                    elif recv_mssg.type == 'ls':
                        if self.machine.nodeId[3] == self.leader_node[3]:
                            replicas = self.leader_work(recv_mssg.type, recv_mssg.kwargs['sdfsfilename'])
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