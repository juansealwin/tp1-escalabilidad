import time
import os
import json
import pika
import logging
import signal
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *

class DecadeFilter():
    AUTHOR_POS = 1
    PUBLISHER_DATE_POS = 3

    def __init__(self):
        self.__init_config()
        time.sleep(10)

        self.author_decades = {}

        self.queue_manager = QueueManager()

        # Queue to receive books_data
        self.queue_manager.setup_receive_queue('books_data', self.__process_message)

        # Queue to send books_data
        self.queue_manager.setup_send_queue('authors_and_decades')

        # Queue to set internal comm between workers
        self.queue_manager.setup_leader_queues(self.id, self.leader, self.total_workers, 'authors_and_decades')

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def __init_config(self):
        init_log()
        self.total_workers = int(os.getenv("TOTAL_WORKERS", '1'))
        self.leader = int(os.getenv("LEADER",'0'))
        self.id = int(os.getenv("ID",'0'))

    

    def __process_message(self, ch, method, properties, body):
        
        line = body.decode('utf-8')

        if line == "END":
            # First protocol msg to result queue
            self.queue_manager.send_message('authors_and_decades', f"{QueryType.QUERY2},{self.total_workers}")

            logging.info(f"Worker_{self.id} propagate end to all {self.total_workers} workers")
            self.queue_manager.propagate_end_message()
            return

        fields = line.split('|')

        author = fields[self.AUTHOR_POS]
        author_len = len(author.strip())
        publisher_date_field = fields[self.PUBLISHER_DATE_POS].strip()

        if not publisher_date_field or author_len == 0:
            return

        year = None

        try:
            year = int(publisher_date_field)

        except ValueError:
            # Format #yyyy-mm-dd
            parts = publisher_date_field.split('-')
            if len(parts) > 0:
                try:
                    year = int(parts[0])

                except ValueError:
                    return
            
         
        if year is not None:
            # Get the decade
            decade = year // 10 * 10

            # Update the map
            if author in self.author_decades:
                if decade not in self.author_decades[author]:
                    self.author_decades[author].append(decade)
            else:
                self.author_decades[author] = [decade]


    def __process_result(self):
        logging.info(f"Worker_{self.id} Processing results...")
        for author, decades in self.author_decades.items():
            decades_str = ' | '.join(str(decade) for decade in decades)
            message = f"{author}, {decades_str}"
            self.queue_manager.send_message('authors_and_decades', message)

        self.queue_manager.send_message('authors_and_decades', "END")    


    def handle_sigterm(self, signum, frame):
        logging.debug('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True


    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('books_data')
