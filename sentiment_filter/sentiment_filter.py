import time
import os
import json
import pika
import logging
import signal
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *
from textblob import TextBlob

class SentimentFilter():
    # POS RATING_DATA
    TITLE_POS_RD = 0
    USER_ID_POS_RD = 1
    TEXT_REVIEW_POS_RD = 5

    def __init__(self):
        self.__init_config()

        self.fiction_books = {}
        self.rating_data = {}
        self.end_fiction_books = False
        
        self.queue_manager = QueueManager()

        self.__setup_queues()

        self.current_query_type = None

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def __init_config(self):
        init_log()


    def __setup_queues(self):
        # Queue to receive fiction books data
        self.queue_manager.setup_receive_queue('fiction_books', self.__process_fiction_books_message)
        # Queue to receive rating data
        self.queue_manager.setup_receive_queue('rating_data', self.__process_rating_data_message)

        # Queue to send result
        self.queue_manager.setup_send_queue('avg_rating_data')

    def __process_fiction_books_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        
        if line == "END":
            logging.info(f'END BOOK DATA')
            self.end_fiction_books = True
            self.__empty_cached_rating_data()
            return

        # Add the a new fiction book to the dict
        # [ Title ] : [ total_score , total_reviews ]
        self.fiction_books[line] = [0, 0]

    def __process_rating_data_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        logging.debug(f"sentiment_filter {line}")

        
        if self.current_query_type is None:
            self.current_query_type = QueryType.validate_query_type(line)
            return
        

        if line == "END":
            logging.info(f'END RATING')
            self.__empty_cached_rating_data()
            result = self.__send_result()
            return

        fields = line.split('|')

        title = fields[self.TITLE_POS_RD]
        user_id = fields[self.USER_ID_POS_RD]
        review = fields[self.TEXT_REVIEW_POS_RD]

        if self.end_fiction_books:
            
            if title in self.fiction_books:
                
                self.__analize_sentiment(title, review)

            self.__empty_cached_rating_data()    

        else:
            self.rating_data[user_id] = {'title': title, 'review': review} 


    def __send_result(self):

        self.queue_manager.send_message('avg_rating_data', f"{QueryType.QUERY5.value}")
        
        for title, data in self.fiction_books.items():
            total_score = data[1]
            total_reviews = data[0]

            self.queue_manager.send_message('avg_rating_data', f"{title}|{total_score}|{total_reviews}")

        self.queue_manager.send_message('avg_rating_data', "END")    


    def __empty_cached_rating_data(self):

        if self.rating_data and self.end_fiction_books:

            for user_id, data in self.rating_data.items():
                title = data['title']
                if title in self.fiction_books:
                    review = data['review']
                    self.__analize_sentiment(title, review)
                    
            self.rating_data = {}        


    def __analize_sentiment(self, title, review):
        blob = TextBlob(review)

        polarities = []

        # sentence.sentiment.polarity [-1 : 1]
        for sentence in blob.sentences:
            polarities.append(sentence.sentiment.polarity)

        # Calculate the avg
        if polarities:
            average_polarity = sum(polarities) / len(polarities)
        else:
            average_polarity = 0.0

        self.fiction_books[title][1] += average_polarity
        self.fiction_books[title][0] += 1


    def handle_sigterm(self, signum, frame):
        logging.info('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True
        self.queue_manager.stop_consuming_all()
        logging.info('action: handle_sigterm | result: success')

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        # Start consuming fiction_books and rating_data
        self.queue_manager.start_consuming('fiction_books')
        self.queue_manager.start_consuming('rating_data')
