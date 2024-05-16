import time
import os
import json
import pika
import logging
import signal
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *

class ReviewCounter():
    TITLE_POS = 0
    RATING_POS = 3

    def __init__(self) -> None:
        self.counter = {}
        
        self.__init_config()
        time.sleep(10)
        self.queue_manager = QueueManager(single_channel=True)
        self.__setup_queues()
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.shutdown_requested = False

    def __init_config(self):
        self.pair_count = int(os.getenv("PAIR_COUNT", '0'))
        self.leader = int(os.getenv("LEADER",'0'))
        self.id = int(os.getenv("ID",'0'))
        init_log()

    def __setup_queues(self):
        self.queue_manager.setup_receive_queue('rating_data',callback=self.__process_message, auto_ack=True, durable=True)
        self.queue_manager.setup_send_queue('review_counter',durable=True)
        self.__setup_leader_queues()
    
    def __setup_leader_queues(self):
        if self.id == self.leader:
            self.queue_manager.setup_receive_queue('leader_finish_rc', callback= self.__process_finish_message, auto_ack=True, durable=True)
        else:
            self.queue_manager.setup_send_queue('leader_finish_rc',durable=True)
            self.queue_manager.setup_receive_queue(f'leader_finish_rc_{self.id}',callback=self.__process_finish_message,auto_ack=True, durable=True)            


    def __process_message(self, ch, method, properties, body):
        if self.shutdown_requested:
            return
        
        line = body.decode('utf-8')
        
        if line == "END":
            logging.info(f"sending ending signal to all {self.pair_count} pairs")
            first_message = f"{QueryType.QUERY3.value},{self.pair_count}"
            logging.info(f"sending query signal to joiner {QueryType.QUERY3.value}")
            self.queue_manager.send_message('review_counter',first_message)
            if self.id != self.leader:
                self.queue_manager.send_message('leader_finish_rc' ,f"END,{self.id}")
                self.__forward_message()
                return
            else:
                self.__leader_message(self.id)
                self.__forward_message()
                return

        fields = line.split('|')
        self.counter.setdefault(fields[self.TITLE_POS], [0,0])
        self.counter[fields[self.TITLE_POS]][0] += 1
        self.counter[fields[self.TITLE_POS]][1] += float(fields[self.RATING_POS])
        logging.debug(f"processed {fields[self.TITLE_POS]}")

    def __process_finish_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        logging.info("finished processing all messages")
        logging.debug(f"{self.counter}")
        if self.id == self.leader:
            self.__leader_message(fields[1])
        else:
            logging.info("Stopping processing")
        
        self.__forward_message()

    def __leader_message(self,sender):
        sender = int(sender)
        logging.info(f"Letting everyone know, sender was {sender}")
        for i in range(0,self.pair_count):
            if i != self.id and i!= sender:
                logging.info(f"Sending to {i}")
                self.queue_manager.setup_send_queue(f'leader_finish_rc_{i}',durable=True)
                self.queue_manager.send_message(f'leader_finish_rc_{i}',f"END")
    
    def __forward_message(self):
        logging.info("forwarding messages")
        for key in self.counter.keys():
            self.queue_manager.send_message('review_counter',f"{key}|{self.counter[key][0]}|{self.counter[key][1]}")
        
        self.counter = {}
        logging.info(f"sending end signal")
        self.queue_manager.send_message('review_counter',f"END")

    def handle_sigterm(self, signum, frame):
        logging.debug('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming_sq()
        
