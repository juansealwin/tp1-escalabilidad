import time
import os
import logging
import socket
import threading
from common.log import init_log
from rabbitmq.queue_manager import *
from common.protocol import QueryType

class Client:
    def __init__(self):
        self.__init_config()
        
        self.queue_manager = QueueManager()
        
        self.received_results = 0

        #Queue to send data
        self.queue_manager.setup_send_exchange('client_request')

        #Queue to receive result
        self.queue_manager.setup_receive_queue('result', self.__process_result)


    def __init_config(self):
        init_log()
        self.current_query_type = os.getenv('QUERY_TYPE')

        if not QueryType.validate_query_type(self.current_query_type):
            raise ValueError(f"Invalid query_type: {self.current_query_type}")

        

    def __process_result(self, ch, method, properties, body):
        line = body.decode('utf-8')
        logging.info(f"[x] Received {line}")    

        if line == "END":
            logging.info(f"Total results received: {self.received_results}")
            logging.info("Received END message. Exiting...")
            os._exit(0)

        else:
            self.received_results = self.received_results + 1


    def send_query_to_queue(self):
        if self.current_query_type:
            logging.info(f'Sending query_type to data_sender queue: {self.current_query_type}')
            self.queue_manager.send_message_exchange('client_request', self.current_query_type)
            
        
        else:
            logging.info(f'query_type was not setted')

    def recv_result(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('result')        
