import os
import paramiko
import logging
import colorlog
import scp as SCP
from typing import Dict
from config import config
from modules.template import base

class Target(object):
    """
    Wrapper around SSHClient and SCPClient connections
    for a remote server. 
    """
    def __init__(self, hostname: str, username: str):
        """ Create SSHClient and SCPClient, and store 
        relevant info; does not connect. User must call
        self.connect()
        """
        self.hostname: str = hostname
        self.username: str = username

    def __connect(self, hostname: str, username: str) -> bool:
        """ Connect this Target to a specific hostname
        using the given username. 
        """
        # create/configure SSHClient
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # attempt SSH connection
        ssh.connect(hostname, username=username)

        # create/configure SCP client
        scp = SCP.SCPClient(ssh.get_transport())

        # save objects
        self.ssh = ssh
        self.scp = scp

        return True

    def put(self, origin: str, dest: str) -> bool:
        """ Copy a file from origin on local to dest
        on remote
        """
        return self.scp.put(origin, dest)

    def get(self, origin: str, dest: str) -> bool:
        """ Copy a file from origin on remote to dest
        on local
        """
        return self.scp.get(origin, dest)

    def connect(self) -> bool:
        """ Connect/reconnect to the last saved hostname/username
        This function must be wrapped in a try/except statement
        in order to catch connection errors. 
        """
        try:
            self.__connect(self.hostname, self.username)
            return True
        except Exception as e:
            return False

    def is_alive(self) -> bool:
        """ Check whether the SSH connection is still alive. 
        """
        try:
            transport = self.ssh.get_transport()
            transport.send_ignore()
            return True
        except Exception as e:
            return False

        return False
        
    
class TransferServer(base.MQTTServer):
    """ 
    This server is responsible for transferring files from `aster` to
    `stars.uchicago.edu`
    """

    def __init__(self):
        """ 
        We initialize the super class (which handles all MQTT configuration)
        and start listening. 
        """

        # MUST INIT SUPERCLASS FIRST
        super().__init__("Transfer Server")

        # create connections to the various servers
        self.aster = Target('telescope.stoneedgeobservatory.com', 'sirius')
        self.aster.connect()
        self.stars = Target('stars.uchicago.edu', 'rprechelt')
        self.stars.connect()
    
        # MUST END WITH start() - THIS BLOCKS
        self.log.info('Transfer Server starting to listen to MQTT messages...')
        self.start()


    def topics(self) -> [str]:
        """ This function must return a list of topics that you wish the server
        to subscribe to. i.e. ['/seo/queue'] etc.
        """

        return ['/'.join(['', config.mqtt.root, 'telescope'])]

    
    def process_message(self, topic: str, msg: Dict[str, str]):
        """ This function is given a JSON dictionary message from the broker
        and must decide how to process the message given the servers purpose. This
        is automatically called whenever a message is received
        """
        # reconnect to aster if the connection is dead
        if not self.aster.is_alive():
            if not self.aster.connect():
                self.log.error(f'Error occured connecting to aster: {e}')

        # reconnect to stars if the connection is dead
        if not self.stars.is_alive():
            if not self.stars.connect():
                self.log.error(f'Error occured connecting to stars: {e}')

        # filename + directory name
        name = os.path.basename(msg['filename'])
        dirname = os.path.dirname(msg['filename'])
        # TODO: this could be improved and is sensitive to changes in the path
        # location on aster; need to better parse correct location
        relpath = '/'.join(dirname.split('/')[4:]) 

        try:
            # transfer file from aster to sirius
            self.aster.get(msg['filename'],
                           f'/tmp/{name}')

            # make sure directory exists on stars
            self.stars.ssh.exec_command(f'mkdir -p /data/public/atlas/{relpath}/')
            
            # transfer file from sirius to sltars
            self.stars.put(f'/tmp/{name}',
                           f'/data/public/atlas/{relpath}/{name}')

            self.log.info(f'Successfully transfered {msg["filename"]} to stars')
            return
        
        except Exception as e:
            self.log.error(f'An error occured transferring {msg["filename"]} to stars')
            self.log.error(e)
            return


    def close(self):
        """ This function is called when the server receives a shutdown
        signal (Ctrl+C) or SIGINT signal from the OS. Use this to close
        down open files or connections. 
        """
        
        # USER [OPTIONAL]
        
        return 
