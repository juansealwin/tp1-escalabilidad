import time
import os
import pika
import logging
import signal
from common.log import init_log

class Joiner:
    TITLE_POS = 0
    COUNT_POS = 1
    RATING_POS = 2

    def __init__(self) -> None:
        self.counter = {}
        self.__init_config()
        time.sleep(10)
        self.__connect_to_rabbitmq()
        self.__setup_queues()
        self.shutdown_requested = False
        self.queue_type = 3

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        self.pairs = int(os.getenv("PAIRS", '0'))
        self.min_count = int(os.getenv("MIN_COUNT",'500'))
        self.finished_pairs = 0
        init_log(log_level)


    def __connect_to_rabbitmq(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                self.__connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(1)

        logging.info(' [*] Connected to RabbitMQ')
        
    def __setup_queues(self):
        self.__channel_result = self.__connection.channel()
        self.__channel_result.queue_declare(queue='result', durable=True)
        self.__channel_counter = self.__connection.channel()
        self.__channel_counter.queue_declare(queue='review_counter',durable=True)
        self.__channel_counter.queue_declare(queue='book_joiner', durable=True)
        self.__channel_counter.basic_consume(queue='review_counter', on_message_callback = self.__process_message, auto_ack=True)


    def __process_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        
        if line == 'END':
            self.finished_pairs +=1
            if self.finished_pairs == self.pairs:
                logging.debug('finished pairs')
                self.__process_books()
            return

        self.counter.setdefault(fields[self.TITLE_POS],[0,0])
        self.counter[fields[self.TITLE_POS]][self.COUNT_POS] += int(fields[self.COUNT_POS])
        ### agregar contador para rating
    
    def __process_books(self):
        self.__channel_counter.basic_consume(queue='book_joiner', on_message_callback = self.__process_book_message, auto_ack=True)

    def __process_book_message(self,ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        logging.info(f" [x] Received {body}")
        
        if line == 'END':
            self.counter = {}
            self.finished_pairs = 0
            return
        
        if self.queue_type == 3:
            if self.counter[fields[self.COUNT_POS]][0] >= self.min_count:
                self.__send_message(self.__channel_result, f"{fields[self.COUNT_POS]}", 'result')

    def __send_message(self, channel, message, routing_key):
        channel.basic_publish(exchange='', routing_key=routing_key, body=message)
        logging.info(f" [x] Sent '{message}'")
    
    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel_counter.start_consuming()


