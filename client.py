import socket
import sys
import threading
import pickle
import random
import datetime
import time
import os
from utils import *
from static import *
import logging
import time
from machine import Machine
from failure_detector import Failure_Detector
from file_system import File_System


MAX = 8192                  # Max size of message   
INIT_STATUS = 'Not Joined'  # Initial status of a node
BASE_FS_PORT = 9000
BASE_PORT = 8000
WRITE_QUORUM = 1

BUFFER_SIZE = 4096

class Client:

    def __init__(self, MACHINE_NUM, STATUS=INIT_STATUS):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)
        self.machine = Machine()

        if self.MACHINE_NUM == 1:
            self.version = time.mktime(datetime.datetime.now().timetuple())
            self.machine.nodeId = (self.ip, self.port, self.version, self.MACHINE_NUM)

        logging.basicConfig(filename=f"vm{self.MACHINE_NUM}.log",
                                        filemode='w',
                                        format='[%(asctime)s | %(levelname)s]: %(message)s',
                                        level=logging.DEBUG)
        self.machine.logger = logging.getLogger(f'vm{self.MACHINE_NUM}.log')

        self.machine.status = 'Joined' if MACHINE_NUM==1 else STATUS
        self.machine.membership_list = MembershipList()  


    def send_message(self, sock_fd, msg):
        ''' Send a message to another machine '''
        try:
            sock_fd.sendall(msg)
        except:
            pass


    def server(self):
        self.fail_detector = Failure_Detector(self.MACHINE_NUM, self.machine)
        self.fail_detector.start_machine()

        time.sleep(4)
        self.file_system = File_System(self.MACHINE_NUM, self.machine)
        self.file_system.start_machine()


    def write_replicas(self, mssg, local_filename, sdfs_filename):
        replica_servers = mssg.kwargs['replica']
        ack_count = 0
        for replica in replica_servers:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((replica[0], replica[1])) 

            # Send filename message to the replicas
            self.send_message(sock_fd, pickle.dumps(sdfs_filename))
            data = sock_fd.recv(MAX)
            print("File opened at replica: ", replica)
            # If file is opened, send the file content
            if "ACK" == pickle.loads(data):

                with open(local_filename, 'rb') as f:
                    bytes_read = f.read()
                    if not bytes_read:
                        break

                    while bytes_read:
                        self.send_message(sock_fd, bytes_read)
                        bytes_read = f.read()
                
                sock_fd.shutdown(socket.SHUT_WR)
                data = sock_fd.recv(MAX)
                mssg = pickle.loads(data)
                sock_fd.close()

                if mssg.type == "ACK":
                    ack_count += 1

        if ack_count >= WRITE_QUORUM:
            print("[ACK Received] Put file successfully\n")
        else:
            print("[ACK Not Received] Put file unsuccessfully\n")            



    def put(self, local_filename, sdfs_filename):
        ''' Put a file in the SDFS '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        host = list(self.machine.membership_list.active_nodes.keys())[0]
        modified_host = (host[0], host[3] + BASE_FS_PORT, host[2], host[3])
        sock_fd.connect((modified_host[0], modified_host[1]))    

        put_mssg = Message(msg_type="put", 
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        )                      

        self.send_message(sock_fd, pickle.dumps(put_mssg))
        print('Message sent')
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "leader":
            print("Leader is: ", mssg.kwargs['leader'])
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((mssg.kwargs['leader'][0], mssg.kwargs['leader'][1]))
            self.send_message(sock_fd, pickle.dumps(put_mssg))
            
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            if mssg.type == "replica":
                print("Replica Servers: ", mssg.kwargs['replica'])
                self.write_replicas(mssg, local_filename, sdfs_filename)
            else:
                print("Unseccessful Attempt\n")

        elif mssg.type == "replica":
            print("Replica Servers: ", mssg.kwargs['replica'])
            self.write_replicas(mssg, local_filename, sdfs_filename)                
        else:
            print("Unsuccessful Attempt\n")          



    def read_replicas(self, mssg, sdfs_filename, local_filename):
        replica = mssg.kwargs['replica']
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_fd.connect((replica[0], replica[1])) 

        # Send filename message to the replicas
        self.send_message(sock_fd, pickle.dumps(sdfs_filename))

        with open(local_filename, 'rb') as f:
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


    def get(self, sdfs_filename, local_filename):
        ''' Get a file from the SDFS '''
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        host = list(self.machine.membership_list.active_nodes.keys())[0]
        modified_host = (host[0], host[3] + BASE_FS_PORT, host[2], host[3])
        sock_fd.connect((modified_host[0], modified_host[1]))    

        put_mssg = Message(msg_type="get", 
                        host=self.machine.nodeId,
                        membership_list=None,
                        counter=None,
                        filename=sdfs_filename,
                        )                      

        self.send_message(sock_fd, pickle.dumps(put_mssg))
        print('Message sent')
        data = sock_fd.recv(MAX)
        mssg = pickle.loads(data)
        sock_fd.close()

        if mssg.type == "leader":
            print("Leader is: ", mssg.kwargs['leader'])
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_fd.connect((mssg.kwargs['leader'][0], mssg.kwargs['leader'][1]))
            self.send_message(sock_fd, pickle.dumps(put_mssg))
            
            data = sock_fd.recv(MAX)
            mssg = pickle.loads(data)
            sock_fd.close()

            if mssg.type == "replica":
                print("Replica Servers: ", mssg.kwargs['replica'])
                self.read_replicas(mssg, sdfs_filename, local_filename)
            else:
                print("Unseccessful Attempt")

        elif mssg.type == "replica":
            print("Replica Servers: ", mssg.kwargs['replica'])
            self.read_replicas(mssg, sdfs_filename, local_filename)  

        elif mssg.type == "NACK":
            print("[ACK not Received] File not found in SDFS\n")


    def client(self):
        ''' Start the client '''
        while True:
            inp = input()

            if inp == "list_mem":
                self.fail_detector.list_mem()

            elif inp == "list_self":
                self.fail_detector.list_self()

            elif inp == "join":
                self.fail_detector.node_join()

            elif inp == "enable suspicion":
                self.fail_detector.enable_suspicion()

            elif inp == "disable suspicion":
                self.fail_detector.disable_suspicion()

            elif inp.startswith("put"):
                _, local_filename, sdfs_filename = inp.split(' ')
                self.put(local_filename, sdfs_filename)

            elif inp.startswith("get"):
                _, sdfs_filename, local_filename = inp.split(' ')
                self.get(sdfs_filename, local_filename)

            elif inp.startswith("delete"):
                _, sdfs_filename = inp.split(' ')
            
            elif inp.startswith("ls"):
                _, sdfs_filename = inp.split(' ')


    def start_machine(self):
        print(f"Machine {self.MACHINE_NUM} Running, Status: {self.machine.status}")

        server_thread = threading.Thread(target=self.server)
        client_thread = threading.Thread(target=self.client)
        server_thread.start()
        client_thread.start()
        server_thread.join()
        client_thread.join()


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]
#     print(MACHINE_NUM)

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()