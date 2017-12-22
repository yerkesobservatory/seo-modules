import sys
import os
import atexit
import json
import time
import paho.mqtt.client as mqtt
from typing import List, Dict


class MQTTServer(object):
    """ This class represents a general purpose server that interacts with the SEO
    ecosystem; subclasses of this server are able to easily extend the feature 
    set of SEO.
    """

    # logger
    log = None

    def __init__(self, name: str):
        """ This creates a new server listening on a user-defined set of topics
        on the MQTT broker specified in config
        """
        self.__name = name

        # initialize logging system
        if not self.__class__.log:
            self.__class__.__init_log()

        # connect to MQTT broker
        self.client = self.__connect()
        self.log.info(f'Creating new {name}...')

        # register atexit handler
        atexit.register(self.__handle_exit)


    def topics(self) -> [str]:
        """ This function must return a list of topics that you wish the server
        to subscribe to. i.e. ['/seo/queue'] etc.
        """

        # USER MUST COMPLETE

        return []

    
    def process_message(self, topic: str, msg: {str}) -> bool:
        """ This function is given a JSON dictionary message from the broker
        and must decide how to process the message given the application. 
        """

        # USER MUST COMPLETE

        return True

    
    def close(self):
        """ This function is called when the server receives a shutdown
        signal (Ctrl+C) or SIGINT signal from the OS. Use this to close
        down open files or connections. 
        """

        return


    def publish(self, topic: str, message: Dict) -> True:
        """ This method converts the message to JSON and publishes
        it on the topic given by topic. 
        """
        self.client.publish(topic, json.dumps(message))
        return True

    
    def __connect(self) -> bool:
        """ Connect to the MQTT broker and return the MQTT client
        object.
        """

        # mqtt client to handle connection
        client = mqtt.Client()

        # server information
        host = config.get('server').get('host') or 'localhost'
        port = config.get('server').get('mosquitto').get('port') or 1883
        name = config.get('general').get('name') or 'Atlas'
        email = config.get('general').get('email') or 'your system administrator'

        # connect to message broker
        try:
            client.connect(host, port, 60)
            self.log.info(f'Successfully connected to {name}')
        except:
            self.log.error(f'Unable to connect to {name}. Please try again later. '
                           f'If the problem persists, please contact {email}')
            print(sys.exc_info())
            exit(-1)

        return client

    
    def start(self):
        """ Starts the servers listening for new requests; server blocks
        on the specified port until it receives a request
        """
        self.client.on_message = self.__process_message
        for topic in self.topics():
            self.client.subscribe(topic)
        self.client.loop_forever()

    
    def __process_message(self, client, userdata, msg) -> List:
        """ This function is called whenever a message is received.
        """
        topic = msg.topic
        msg = json.loads(msg.payload.decode())
        self.process_message(topic, msg)

        
    def __handle_exit(self, *_):
        """ Registered with atexit to call
        the user completed function self.close()
        """
        self.log.info('Closing down...')

        try:

            # call user close function
            self.close()

            # close MQTT connection
            self.client.disconnect()
        finally:
            # quit the process
            exit(1)


    @classmethod
    def __init_log(cls) -> bool:
        """ Initialize the logging system for this module and set
        a ColoredFormatter. 
        """
        # create format string for this module
        format_str = config.logging.fmt.replace('[name]', 'TELESCOPE SERVER')
        formatter = colorlog.ColoredFormatter(format_str, datefmt=config.logging.datefmt)

        # create stream
        stream = logging.StreamHandler()
        stream.setLevel(logging.DEBUG)
        stream.setFormatter(formatter)

        # assign log method and set handler
        cls.log = logging.getLogger('telescope_server')
        cls.log.setLevel(logging.DEBUG)
        cls.log.addHandler(stream)

        return True
