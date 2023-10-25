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


INIT_STATUS = 'Not Joined'  # Initial status of a node


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
        self.fail_detector = Failure_Detector(MACHINE_NUM, self.logger, self.membership_list, self.status)
        self.fail_detector.start_machine()

        self.file_system = File_System(MACHINE_NUM, self.logger, self.membership_list, self.status)
        self.file_system.start_machine()
        

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
                _, filename, sdfs_filename = inp.split()
                # TODO: Read contents of filename and send the contents
                self.file_system.put(sdfs_filename)

    def client(self):
        ''' Start the client '''
        command_thread = threading.Thread(target=self.command)

        command_thread.start()
        command_thread.join()


    def start_machine(self):
        print(f"Machine {self.MACHINE_NUM} Running, Status: {self.status}")

        server_thread = threading.Thread(target=self.server)
        client_thread = threading.Thread(target=self.client)

        server_thread.start()
        client_thread.start()
        server_thread.join()
        client_thread.join()


if __name__ == "__main__":
    MACHINE_NUM = sys.argv[1]

    machine = Machine(int(MACHINE_NUM))
    machine.start_machine()