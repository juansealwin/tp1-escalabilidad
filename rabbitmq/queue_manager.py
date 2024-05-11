import pika
import logging

class QueueManager:
    def __init__(self, connection=None, host='rabbitmq'):

        self.host = host
        self.channels = {} 
        self.__connect()


    def __connect(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(1)

        logging.info(' [*] Connected to RabbitMQ')        

    def setup_send_queue(self, queue_name, durable=True):
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=durable)
        self.channels[queue_name] = channel 

    def setup_receive_queue(self, queue_name, callback, durable=True, auto_ack=False):
        channel = self.connection.channel()
        channel.queue_declare(queue=queue_name, durable=durable)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
        self.channels[queue_name] = channel

    def send_message(self, queue_name, message):
        channel = self.channels.get(queue_name)
        if channel:
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)
        else:
            logging.error(f'send_message: Queue "{queue_name}" not found.')

    def start_consuming(self, queue_name):
        channel = self.channels.get(queue_name)
        if channel:
            channel.start_consuming()
        else:
            logging.error(f'start_consuming: Queue "{queue_name}" not found.')
