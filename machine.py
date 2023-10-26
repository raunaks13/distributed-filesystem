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
from failure_detector import Failure_Detector
from file_system import File_System


MAX = 8192                  # Max size of message   
INIT_STATUS = 'Not Joined'  # Initial status of a node
BASE_PORT = 8000


class Machine:

    def __init__(self, MACHINE_NUM, STATUS=INIT_STATUS):
        self.MACHINE_NUM = MACHINE_NUM
        self.port = BASE_PORT + MACHINE_NUM
        self.hostname = "fa23-cs425-37" + f"{MACHINE_NUM:02d}" + ".cs.illinois.edu"
        self.ip = socket.gethostbyname(self.hostname)

        if self.MACHINE_NUM == 1:
            self.version = time.mktime(datetime.datetime.now().timetuple())
            self.nodeId = (self.ip, self.port, self.version)

        logging.basicConfig(filename=f"vm{self.MACHINE_NUM}.log",
                                        filemode='w',
                                        format='[%(asctime)s | %(levelname)s]: %(message)s',
                                        level=logging.DEBUG)
        self.logger = logging.getLogger(f'vm{self.MACHINE_NUM}.log')

        self.status = 'Joined' if MACHINE_NUM==1 else STATUS
        self.membership_list = MembershipList()  

    def server(self):
        pass


    def command(self):
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
                # Read local_file and send the contents of the leader
                self.file_system.put(local_filename, sdfs_filename)

            elif inp.startswith("get"):
                _, sdfs_filename, local_filename = inp.split(' ')

            elif inp.startswith("delete"):
                _, sdfs_filename = inp.split(' ')
            
            elif inp.startswith("ls"):
                _, sdfs_filename = inp.split(' ')

    def client(self):
        ''' Start the client '''
        command_thread = threading.Thread(target=self.command)

        command_thread.start()
        command_thread.join()


    def start_machine(self):
        print(f"Machine {self.MACHINE_NUM} Running, Status: {self.status}")

        self.fail_detector = Failure_Detector(self.MACHINE_NUM, self.logger, self.membership_list, self.status)
        self.fail_detector.start_machine()

        self.file_system = File_System(self.MACHINE_NUM, self.logger, self.membership_list, self.status)
        self.file_system.start_machine()

        # server_thread = threading.Thread(target=self.server)
        client_thread = threading.Thread(target=self.client)
        # server_thread.start()
        client_thread.start()
        # server_thread.join()
        client_thread.join()


# if __name__ == "__main__":
#     MACHINE_NUM = sys.argv[1]
#     print(MACHINE_NUM)

#     machine = Machine(int(MACHINE_NUM))
#     machine.start_machine()