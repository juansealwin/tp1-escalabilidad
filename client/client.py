import os
import pika
import time
import logging
import csv
import os
from configparser import ConfigParser
from common.log import init_log

class Client:
    def __init__(self):
        self.config = self.__init_config()
        self.__init_log()
        time.sleep(10)

    def __init_config(self):
        """ Parse env variables or config file to find program config params"""
        config = ConfigParser(os.environ)
    
        config_params = {}
        try:
            config_params["books_data_file"] = os.getenv('BOOKS_DATA_FILE', config["DEFAULT"]["BOOKS_DATA_FILE"])
            config_params["books_rating_file"] = os.getenv('BOOKS_RATING_FILE', config["DEFAULT"]["BOOKS_RATING_FILE"])

        except KeyError as e:
            raise KeyError("Key was not found. Error: {} .Aborting client".format(e))

        except ValueError as e:
            raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

        return config_params

    def __init_log(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        init_log(log_level)

    def __send_message(self, channel, message):
        channel.basic_publish(exchange='', routing_key='books_analizer_data', body=message)
        logging.debug(f" [x] Sent '{message}'")

    def __filter_line(self, line):
        # Discard fileds descripcion[1], image[3], previewLink[4], infoLink[7]
        filtered_fields = [line[0], line[2], line[5], line[6], line[8], line[9]]
        filtered_line = ','.join(filtered_fields)
        return filtered_line

    def process_books_data(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(2)

        logging.info(' [*] Connected to RabbitMQ')
        channel = connection.channel()
        channel.queue_declare(queue='books_analizer_data', durable=True)

        file_name = self.config["books_data_file"]

        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                for line in reader:
                    msg = self.__filter_line(line)
                    self.__send_message(channel, msg)

        else:
            logging.info(f' [!] File not found: {file_name}')

        if connection.is_open:
            connection.close()