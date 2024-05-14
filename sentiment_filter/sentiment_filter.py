import time
import os
import json
import pika
import logging
import signal
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *

class SentimentFilter():
    AUTHOR_POS = 1
    PUBLISHER_DATE_POS = 3

    def __init__(self):
        self.__init_config()
        time.sleep(10)

        self.ficton_books = {}

        self.queue_manager = QueueManager()

        self.__setup_queues()

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def __init_config(self):
        init_log()


    def __setup_queues(self):
        # Queue to receive books_data
        self.queue_manager.setup_receive_queue('ficton_books', self.__process_message)
        # Queue to receive books_data
        #self.queue_manager.setup_receive_queue('rating_data', self.__process_message)

        # Queue to send books_data
        self.queue_manager.setup_send_queue('result')
    

    def __process_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        
        if line == "END":
            logging.info(f"{self.ficton_books}")

        # Add the a new fiction book to the dict
        # [ Title ] : [ total_score , total_reviews ]
        self.ficton_books[line] = [0, 0]


    def handle_sigterm(self, signum, frame):
        logging.debug('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True


    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('ficton_books')
