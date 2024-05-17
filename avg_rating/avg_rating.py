import signal
import pika
import time
import logging
import os
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import QueueManager

class AvgRating:
    TITLE_POS = 0
    TOT_REVIEWS_POS = 1
    TOT_SCORE_POS = 2
    TOP_PERCENTILE = 0.1

    def __init__(self):
        init_log()         
        self.queue_manager = QueueManager()
        self.__setup_queues()
        
        self.titles_rating = []
        self.current_query_type = None
        signal.signal(signal.SIGTERM, self.handle_sigterm)


    def __setup_queues(self):
        # Queue to receive rating data
        self.queue_manager.setup_receive_queue('avg_rating_data', self._process_message)

        # Queue to send result
        self.queue_manager.setup_send_queue('result')
         

    def __set_current_query_type(self, line):
        for query_type in QueryType:
            if line == query_type.value:
                self.current_query_type = query_type.value
                return  
    
        logging.info(f"[!] Wrong first message: {line}...") 

    def _process_message(self, ch, method, properties, body):

        line = body.decode('utf-8')

        if line == "END":
            self._send_result()

        elif self.current_query_type is None:
            self.__set_current_query_type(line)

        else:  
             
            title, ratio = self._parse_message(line)

            if self.current_query_type == QueryType.QUERY4.value:
                self._update_top_titles(title, ratio)
            
            else:
                self.titles_rating.append((title, ratio))

            self.titles_rating.sort(key=lambda x: x[1], reverse=True)    


    def _parse_message(self, line):
        fields = line.split('|')
        title = fields[self.TITLE_POS]
        total_reviews = int(fields[self.TOT_REVIEWS_POS])
        total_score = float(fields[self.TOT_SCORE_POS])
        if total_reviews > 0:
            ratio = total_score / total_reviews
        else:
            ratio = 0

        return title, ratio


    def _update_top_titles(self, title, ratio):
        if len(self.titles_rating) < 10:
            self.titles_rating.append((title, ratio))

        else:
            min_title = self.titles_rating[-1]
            if ratio > min_title[1]:
                self.titles_rating.pop()
                self.titles_rating.append((title, ratio))


    def _send_result(self):

        if self.current_query_type == QueryType.QUERY4.value:
            self._send_result_fixed()
        
        else:
            self._send_result_percentage()


        self.queue_manager.send_message('result', "END") 
        self.titles_rating = []
        self.current_query_type = None


    def _send_result_fixed(self):
        for title, ratio in self.titles_rating:
            msg = f"{title},{ratio}"
            self.queue_manager.send_message('result', msg) 

    def _send_result_percentage(self):
        top_percent = int(len(self.titles_rating) * self.TOP_PERCENTILE)
        top_titles = self.titles_rating[:top_percent]

        for title, ratio in top_titles:
            msg = f"{title},{ratio}"
            self.queue_manager.send_message('result', msg) 

    def handle_sigterm(self, signum, frame):
        logging.info('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True
        self.queue_manager.stop_consuming_all()
        logging.info('action: handle_sigterm | result: success')
        
    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('avg_rating_data')
