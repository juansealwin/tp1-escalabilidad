import pika
import time
import logging
import os
from common.log import init_log
from rabbitmq.rabbit_connection import RabbitConnection
from rabbitmq.rabbit_queue import RabbitQueue

class AvgRating:
    TITLE_POS = 0
    TOT_REVIEWS_POS = 1
    TOT_SCORE_POS = 2 

    def __init__(self):
        self._init_config()
        time.sleep(10)

        self.rabbit_conn = RabbitConnection()
        self.rabbit_conn.connect()
        
        # Queue to receive rating data
        self.rating_data_queue = RabbitQueue(self.rabbit_conn.connection, 'rating_data')
        self.rating_data_queue.setup_receive_queue(self._process_message)

        # Queue to send result
        self.result_queue = RabbitQueue(self.rabbit_conn.connection, 'result_data')
        self.result_queue.setup_send_queue()

        self.titles_rating = []
        self.fixed_result = None
        

    def _init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        init_log(log_level)    


    def _process_message(self, ch, method, properties, body):
        logging.debug(f" [x] Received {body}")

        line = body.decode('utf-8')

        if line == "END":
            self._send_result()

        elif self.fixed_result is None:
            if line == "Query5":
                self.fixed_result = False
            else:
                self.fixed_result = True  

        else:   
            title, ratio = self._parse_message(line)

            if self.fixed_result:
                self._update_top_titles(title, ratio)
            
            else:
                self.titles_rating.append((title, ratio))

            self.titles_rating.sort(key=lambda x: x[1], reverse=True)    


    def _parse_message(self, line):
        fields = line.split(',')
        title = fields[self.TITLE_POS]
        total_reviews = int(fields[self.TOT_REVIEWS_POS])
        total_score = float(fields[self.TOT_SCORE_POS])
        ratio = total_score / total_reviews

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
        for title, ratio in self.titles_rating:
            msg = f"{title},{ratio}"
            self.result_queue.basic_publish(msg) 

        self.titles_rating = []
        self.fixed_result = None
        

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.rating_data_queue.start_consuming()
