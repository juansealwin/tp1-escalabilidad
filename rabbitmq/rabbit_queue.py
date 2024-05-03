import pika
import logging

class RabbitQueue:
    def __init__(self, connection, queue_name):
        self.__connection = connection
        self.__queue_name = queue_name
        self.__channel = None

    def setup_send_queue(self, durable=True):
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=self.__queue_name, durable=durable)

    def setup_receive_queue(self, callback, durable=True, auto_ack=False):
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=self.__queue_name, durable=durable)
        self.__channel.basic_consume(queue=self.__queue_name, on_message_callback=callback, auto_ack=auto_ack)

    def send_message(self, message):
        self.__channel.basic_publish(exchange='', routing_key=self.__queue_name, body=message)

    def start_consuming(self):
        self.__channel.start_consuming()