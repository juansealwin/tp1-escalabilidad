import multiprocessing
import threading
import time
import os
import pika
import logging
import signal
from common.log import init_log
from rabbitmq.queue_manager import *

class Joiner:
    TITLE_POS = 0
    AUTHOR_POS = 1
    COUNT_POS = 1
    RATING_POS = 2

    def __init__(self) -> None:
        self.counter = {}
        self.__init_config()
        time.sleep(10)
        self.queue_manager = QueueManager()
        self.__setup_queues()

        self.shutdown_requested = False
        self.queue_type = 3
        self.event = multiprocessing.Event()

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        self.pairs = int(os.getenv("PAIRS", '0'))
        self.min_count = int(os.getenv("MIN_COUNT",'500'))
        self.finished_pairs = 0
        init_log(log_level)

        
    def __setup_queues(self):
        self.queue_manager.setup_receive_queue('review_counter', callback=self.__process_message, auto_ack=True, durable=True)
        self.queue_manager.setup_send_queue('result', durable=True)


    def __process_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split('|')
        logging.debug(f" [x] Received {fields}")
        
        if line == 'END':
            self.finished_pairs +=1
            if self.finished_pairs == self.pairs:
                logging.debug('finished pairs')
                self.__process_books()
            return

        self.counter.setdefault(fields[self.TITLE_POS],[0,0])
        self.counter[fields[self.TITLE_POS]][0] += int(fields[self.COUNT_POS])
        ### agregar contador para rating
    
    def __process_books(self):
        logging.debug("Start book analyzer")
        self.event.set()

    def __process_book_message(self,ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split('|')
        logging.info(f" [x] Received {body}")
        
        if line == 'END':
            self.counter = {}
            self.finished_pairs = 0
            return
        
        if self.queue_type == 3:
            if self.counter.get(fields[self.TITLE_POS], [0])[0] >= self.min_count:
                self.queue_manager.send_message('result', f'found {fields[self.TITLE_POS]}')
    

    def consume_books(self):
        logging.debug("request book")
        self.event.wait()
        logging.debug("aquired book")
        self.new_queueManager= QueueManager()
        self.new_queueManager.setup_receive_queue('book_joiner', callback=self.__process_book_message,auto_ack=True, durable=True)
        logging.debug("setted up queue for book")
        self.new_queueManager.start_consuming('book_joiner')
            
    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.review_counter = multiprocessing.Process(target=self.queue_manager.start_consuming,args=('review_counter',))
        self.review_counter.start()
        self.books = multiprocessing.Process(target=self.consume_books)
        self.books.start()



