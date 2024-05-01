import time
import os
import pika
import logging
import signal
from common.log import init_log

class Joiner:
    TITLE_POS = 0
    COUNT_POS = 1
    RATING, POS = 2

    def __init__(self) -> None:
        self.counter = {}
        self.__init_config()
        time.sleep(10)
        self.__connect_to_rabbitmq()
        self.__setup_queues()
        self.shutdown_requested = False

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        self.pairs = int(os.getenv("PAIRS", '0'))
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
        self.__channel_result = self.connection.channel()
        self.__channel_result.queue_declare(queue='result', durable=True)
        self.__channel_counter = self.connection.channel()
        self.__channel_counter.queue_declare(queue='review_counter')
        self.__channel_counter.basic_consume(queue='review_counter', on_message_callback = self.process_message, auto_ack=True)

    def __process_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        
        if line == 'END':
            self.finished_pairs +=1
            if self.finished_pairs == self.pairs:
                #process_books
                pass
            return

        self.counter.setdefault(fields[self.TITLE_POS],[0,0])
        


