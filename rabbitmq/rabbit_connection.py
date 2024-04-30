import pika
import logging

class RabbitConnection:
    def __init__(self, host='rabbitmq'):
        self.host = host
        self.connection = None

    def connect(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(1)
        logging.info(' [*] Connected to RabbitMQ')
