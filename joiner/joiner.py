import time
import os
import pika
import logging
import signal
from common.log import init_log
from rabbitmq.queue_manager import *
from common.protocol import QueryType

class Joiner:
    TITLE_POS = 0
    COUNT_POS = 1
    RATING_POS = 2

    def __init__(self):
        
        self.__init_config()
        time.sleep(10)
        self.counter = {}

        self.author_decades = {}

        # The type of the current query
        self.current_query_type = None

        self.queue_manager = QueueManager()

        # Queue to receive authors_and_decades
        self.queue_manager.setup_receive_queue('authors_and_decades', self.__process_message)

        # Queue to receive review_counter
        self.queue_manager.setup_receive_queue('review_counter', self.__process_message)

        # TODO: Queue to send book_joiner
        #self.queue_manager.setup_send_queue('book_joiner')

        # Queue to send result
        self.queue_manager.setup_send_queue('result')

    def __init_config(self):
        init_log()
        self.total_workers = int(os.getenv("TOTAL_WORKERS", '0'))
        self.min_reviews = int(os.getenv("MIN_REVIEWS",'500'))
        self.current_query_type = os.getenv("QUERY_TYPE", QueryType.QUERY2)
        self.total_input_workers = 0
        self.finished_workers = 0


    def __set_current_query_type(self, line):
        fields = line.split(',')
        
        for query_type in QueryType:
            if fields[0] == query_type.value:
                self.current_query_type = query_type
                self.total_input_workers = int(fields[1])
                return
            else:
                logging.info(f"query_type {self.current_query_type}, total_input_workers {self.total_input_workers}...")    
    
        logging.info(f"[!] Wrong first message: {line}...")        


    def __process_message(self, ch, method, properties, body):

        # Decode the msg
        line = body.decode('utf-8')

        if self.current_query_type is None:
            self.__set_current_query_type(line)
            
        # TODO: change for each type of query    
        elif line == "END":
            if self.total_input_workers == self.finished_workers:
                self.__process_result_query2() 
            else:    
                self.finished_workers = self.finished_workers + 1

        else: 
            if self.current_query_type == QueryType.QUERY2:
                self.__process_message_query2(line)                
            else:
                self.__process_message_query3(line)

        

    def __process_message_query2(fields):
        author, decades_str = line.split(',')
        decades = [int(decade) for decade in decades_str.split('|')]

        if author in self.author_decades:
            existing_decades = self.author_decades[author]
            for decade in decades:
                if decade not in existing_decades:
                    existing_decades.append(decade)
        else:
            self.author_decades[author] = decades

        logging.info(f"Joiner: Updated author and decades: {author} {decades}")    


    def __process_result_query2(fields):
        for author, decades in self.author_decades.items():
            unique_decades = set(decades)
            if len(unique_decades) > 10:
                authors_with_more_than_10_decades.append(author)
                self.queue_manager('result', author)

        self.queue_manager('result', "END")        

    def __process_message_query3(fields):
        line = body.decode('utf-8')
        fields = line.split('|')
        logging.debug(f" [x] Received {body}")
        
        if line == 'END':
            self.finished_workers +=1
            if self.finished_workers == self.pairs:
                logging.debug('finished pairs')
                self.__process_books()
            return

        self.counter.setdefault(fields[self.TITLE_POS],[0,0])
        self.counter[fields[self.TITLE_POS]][0] += int(fields[self.COUNT_POS])
        ### agregar contador para rating
    
    
    def __process_books(self):
        self.__channel_counter.basic_consume(queue='book_joiner', on_message_callback = self.__process_book_message, auto_ack=True)

    def __process_book_message(self,ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        logging.info(f" [x] Received {body}")
        
        if line == 'END':
            self.counter = {}
            self.finished_workers = 0
            return
        
        if self.current_query_type == QueryType.QUERY3:
            if self.counter[fields[self.COUNT_POS]][0] >= self.min_count:
                self.queue_manager.send_message('result', f"{fields[self.COUNT_POS]}")

    
    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('authors_and_decades')
        self.queue_manager.start_consuming('review_counter')



