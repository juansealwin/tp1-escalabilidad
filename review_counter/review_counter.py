import time
import os
import json
import pika
import logging
import signal
from common.log import init_log


class ReviewCounter():
    TITLE_POS = 0

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
        self.pair_count = int(os.getenv("PAIR_COUNT", '0'))
        self.leader = int(os.getenv("LEADER",'0'))

        logging.info(self.leader)
        self.id = int(os.getenv("ID",'0'))
        logging.info(self.id)


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
        self.__channel.queue_declare(queue='rating_data', durable=True)
        self.__channel.basic_consume(queue='rating_data', on_message_callback=self.__process_message, auto_ack=True)
        self.__setup_leader_queues()
    
    def __setup_leader_queues(self):
        if self.id == self.leader:
            logging.info("Leader setting up channels")
            self.__channel__finish = self.__connection.channel()
            self.__channel__finish.queue_declare(queue='leader_finish')
            self.__channel__finish.basic_consume(queue='leader_finish' , on_message_callback = self.__process_finish_message, auto_ack = True)
        else:
            logging.info("Setting up finish channel")
            self.__channel__leader = self.__connection.channel()
            self.__channel__leader.queue_declare(queue='leader_finish')
            self.__channel__finish = self.__connection.channel()
            self.__channel__finish.queue_declare(queue=f'leader_finish_{self.id}')
            self.__channel__finish.basic_consume(queue=f'leader_finish_{self.id}',on_message_callback = self.__process_finish_message, auto_ack = True)


    def __process_message(self, ch, method, properties, body):
        if self.shutdown_requested:
            return
        logging.debug(f"received {body}")
        line = body.decode('utf-8')
        if line == "END":
            logging.info(f"sending ending signal to all {self.pair_count} pairs")
            if self.id != self.leader:
                self.__send_message(self.__channel__leader, f"END,{self.id}", 'leader_finish')
            else:
                self.__leader_message(self.id)

        fields = line.split(',')
        self.counter.setdefault(fields[self.TITLE_POS], 0)
        self.counter[fields[self.TITLE_POS]] += 1

    def __process_finish_message(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        logging.info("finished processing all messages")
        logging.debug(f"{self.counter}")
        if self.id == self.leader:
            self.__leader_message(fields[1])
        else:
            logging.info("Stopping processing")

    def __leader_message(self,sender):
        logging.info("Letting everyone know")
        for i in range(0,self.pair_count):
            if i != self.id and i!= sender:
               actual_connection = self.__connection.channel()
               actual_channel = actual_connection.queue_declare(queue=f'leader_finish_{i}')
               self.__send_message(actual_connection, f"END", f'leader_finish_{i}')

    def __send_message(self, channel, message, routing_key):
        channel.basic_publish(exchange='', routing_key=routing_key, body=message)
        logging.info(f" [x] Sent '{message}'")

    def handle_sigterm(self, signum, frame):
        logging.debug('action: handle_sigterm | result: in_progress')
        self.shutdown_requested = True
        logging.info(f"{self.counter}")


    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel.start_consuming()
