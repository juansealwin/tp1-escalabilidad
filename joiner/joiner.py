import multiprocessing
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
    AUTHOR_POS = 1
    COUNT_POS = 1
    RATING_POS = 2
    MIN_DIFF_DECADES = 9
    SCORE_POS = 2

    def __init__(self):
        
        self.__init_config()
        time.sleep(10)
        self.counter = {}
        self.queue = multiprocessing.Queue()

        self.author_decades = {}

        # The type of the current query
        self.current_query_type = None

        self.queue_manager = QueueManager()

        self.__setup_queues()

        self.event = multiprocessing.Event()

    def __init_config(self):
        init_log()
        self.total_workers = int(os.getenv("TOTAL_WORKERS", '0'))
        self.min_reviews = int(os.getenv("MIN_REVIEWS",'500'))
        self.current_query_type = os.getenv("QUERY_TYPE", QueryType.QUERY2.value)
        self.total_input_workers = 0
        self.finished_workers = 0

    def __setup_queues(self):
        # Queue to receive authors_and_decades
        self.queue_manager.setup_receive_queue('authors_and_decades', self.__process_message)

        # Queue to receive review_counter
        self.queue_manager.setup_receive_queue('review_counter', callback=self.__process_message, auto_ack=True, durable=True)

        # Queue to send result
        self.queue_manager.setup_send_queue('result')

        self.queue_manager.setup_send_queue('avg_rating_data')



    def __set_current_query_type(self, line):
        fields = line.split(',')
        for query_type in QueryType:
            if fields[0] == query_type.value:
                self.current_query_type = query_type.value
                self.total_input_workers = int(fields[1])
                logging.info(f"query_type {self.current_query_type}, total_input_workers {self.total_input_workers}...") 
                return
    
        logging.info(f"[!] Wrong first message: {line}...")        


    def __process_message(self, ch, method, properties, body):

        # Decode the msg
        line = body.decode('utf-8')
        
        logging.info(f"Joiner: {line}") 
        
        if self.current_query_type is None:
            self.__set_current_query_type(line)
        
        # TODO: change for each type of query    
        elif line == "END":
            logging.info(f"Joiner: END arrived...") 
            if (self.total_input_workers - 1) == self.finished_workers:
                if self.current_query_type == QueryType.QUERY2.value:
                    logging.info(f"Joiner: starting proccesing result of Query2...") 
                    self.__process_result_query2() 
                else:
                    logging.info(f"Joiner: starting proccesing result of Query3...") 
                    self.__process_books()

            else:    
                self.finished_workers = self.finished_workers + 1

        else: 
            if self.current_query_type == QueryType.QUERY2.value:
                self.__process_message_query2(line)                
            else:
                self.__process_message_query3(line)

        

    def __process_message_query2(self, line):        
        parts = line.split('|')
        # Could be several authors
        #authors = [author.strip(" '[]") for author in parts[0].split(',')]
        authors = parts[0].split(',')
        decades_str = parts[1].strip()
        decades = [int(decade) for decade in decades_str.split(',')]

        for author in authors:
            if author in self.author_decades:
                existing_decades = self.author_decades[author]
                for decade in decades:
                    if decade not in existing_decades:
                        existing_decades.append(decade)
            else:
                self.author_decades[author] = decades.copy()

        #logging.info(f"Joiner: Updated authors and decades: {authors} {decades}")   


    def __process_result_query2(self):
        #logging.info(f"{self.author_decades}")

        for author, decades in self.author_decades.items():
            unique_decades = set(decades)
            if len(unique_decades) > self.MIN_DIFF_DECADES:
                #logging.info(f"Joiner: Enviando el siguiente autor: {author} {decades}") 
                self.queue_manager.send_message('result', f'{author},{len(unique_decades)}')

        self.queue_manager.send_message('result', "END")        

    def __process_message_query3(self, line):
        fields = line.split('|')
        logging.debug(f" [x] Received {line}")

        title = fields[self.TITLE_POS]
        count = int(fields[self.COUNT_POS])
        score = float(fields[self.SCORE_POS]) 

        if title in self.counter:
            self.counter[title][0] += count
            self.counter[title][1] += score
        else:
            self.counter[title] = [count, score]
        return
    
    
    def __process_books(self):
        self.queue.put(self.counter)
        logging.debug("Start book analyzer")
        self.event.set()

    def __process_book_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split('|')

        if self.current_query_type is None:
            self.current_query_type = QueryType.validate_query_type(line)
            if self.current_query_type == QueryType.QUERY4.value:
                self.queue_manager.send_message('avg_rating_data',self.current_query_type)

        if line == 'END':
            logging.info('sending end')
            self.counter = {}
            self.finished_workers = 0
            if self.current_query_type == QueryType.QUERY3.value:
                self.queue_manager.send_message('result','END')
            else:
                logging.info('sending end')
                self.queue_manager.send_message('avg_rating_data','END')
            return
        
        #if self.current_query_type == QueryType.QUERY3.value:
        count,score = self.counter.get(fields[self.TITLE_POS], [0,0])
        if count >= self.min_reviews:
            if self.current_query_type == QueryType.QUERY3.value:
                self.queue_manager.send_message('result', f"{fields[self.TITLE_POS]}|{fields[self.AUTHOR_POS]}|{count}")
            else:
                self.queue_manager.send_message('avg_rating_data', f"{fields[self.TITLE_POS]}|{count}|{score}")
    

    def consume_books(self):
        logging.debug("request book")
        self.event.wait()
        logging.debug("aquired book")
        self.counter = self.queue.get()
        self.new_queueManager= QueueManager()
        self.current_query_type = None
        self.new_queueManager.setup_receive_queue('book_joiner', callback=self.__process_book_message,auto_ack=True, durable=True)
        self.new_queueManager.start_consuming('book_joiner')

    
    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        def consume():
            self.queue_manager.start_consuming('authors_and_decades')
            self.queue_manager.start_consuming('review_counter')
        self.consume = multiprocessing.Process(target = consume)
        self.consume.start()
        self.books = multiprocessing.Process(target=self.consume_books)
        self.books.start()

    
    # def run(self):
    #     logging.info(' [*] Waiting for messages. To exit press CTRL+C')
    #     self.author_decades = multiprocessing.Process(target=self.queue_manager.start_consuming,args=('authors_and_decades',))
    #     self.author_decades.start()
    #     self.review_counter = multiprocessing.Process(target=self.queue_manager.start_consuming,args=('review_counter',))
    #     self.review_counter.start()
    #     self.books = multiprocessing.Process(target=self.consume_books)
    #     self.books.start()



