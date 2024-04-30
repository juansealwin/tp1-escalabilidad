import pika
import logging

class RabbitQueue:
    def __init__(self, connection, queue_name):
        self.connection = connection
        self.queue_name = queue_name
        self.channel = None

    def create_send_queue(self, durable=True):
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=durable)

    def create_receive_queue(self, callback, durable=True, auto_ack=False):
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=durable)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=auto_ack)

    def start_consuming(self):
        self.channel.start_consuming()