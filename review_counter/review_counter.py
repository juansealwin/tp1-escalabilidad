import time
import os
import json
import pika
import logging
import signal
from common.log import init_log


class ReviewCounter():
    TITLE_POS = 1

    def __init__(self) -> None:
        self.counter = {}
        self.__init_config()
        time.sleep(10)
        self.__connect_to_rabbitmq()
        self.__setup_queues()
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.shutdown_requested = False

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
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
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(
            queue='review_analizer_data', durable=True)
        self.__channel.basic_consume(
            queue='review_analizer_data', on_message_callback=self.__process_message, auto_ack=True)

    def __process_message(self, ch, method, properties, body):
        if self.shutdown_requested:
            return

        line = body.decode('utf-8')
        fields = line.split(',')
        self.counter.setdefault(fields[self.TITLE_POS], 0)
        self.counter[fields[self.TITLE_POS]] += 1

    def handle_sigterm(self, signum, frame):
        logging.debug('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True
        logging.info(f"{self.counter}")


    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel.start_consuming()
