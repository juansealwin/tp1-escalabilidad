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
        self.__connect_to_rabbitmq()
        self.__setup_queues()

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

    def __connect_to_rabbitmq(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(1)

        logging.info(' [*] Connected to RabbitMQ')

    def __setup_queues(self):
        # Queue to send book data
        self.__channel_book_data = self.__connection.channel()
        self.__channel_book_data.queue_declare(queue='books_data', durable=True)

        # Queue to send rating data
        self.__channel_rating_data = self.__connection.channel()
        self.__channel_rating_data.queue_declare(queue='rating_data', durable=True)

        # Queue to receive result
        self.__channel_result = self.__connection.channel()
        self.__channel_result.queue_declare(queue='result', durable=True)
        self.__channel_result.basic_consume(queue='result', on_message_callback=self.__process_result)

    def __process_result(self, ch, method, properties, body):
        logging.info(f" [x] Received {body}")    

    def __send_message(self, channel, message, routing_key):
        channel.basic_publish(exchange='', routing_key=routing_key, body=message)
        logging.debug(f" [x] Sent '{message}'")


    def __recv_message(self, channel, message):
        self.__channel_result.start_consuming()
        logging.debug(f" [x] Sent '{message}'")

    def __filter_book_data_line(self, line):
        # Books data header: 
        # 'Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount'
        # Discard fileds descripcion[1], image[3], previewLink[4], infoLink[7]
        filtered_fields = [line[0], line[2], line[5], line[6], line[8], line[9]]
        filtered_line = ','.join(filtered_fields)
        return filtered_line

    def __filter_rating_data_line(self, line):
        # Ratings data header: 
        # 'Id, Title, Price, User_id, ProfileName, review/helpfulness, review/score, review/time, review/summary, review/text'
        # Discard fileds Id[0], Price[2], ProfileName[4], review/time[7]
        filtered_fields = [line[1], line[3], line[5], line[6], line[8], line[9]]
        filtered_line = ','.join(filtered_fields)
        return filtered_line

    def send_books_data(self):

        file_name = self.config["books_data_file"]

        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                for line in reader:
                    msg = self.__filter_book_data_line(line)
                    self.__send_message(self.__channel_book_data, msg, 'books_data')

        else:
            logging.info(f' [!] File not found: {file_name}')


    def send_rating_data(self):
        file_name = self.config["books_rating_file"]

        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                for line in reader:
                    msg = self.__filter_rating_data_line(line)
                    self.__send_message(self.__channel_rating_data, msg, 'rating_data')

        else:
            logging.info(f' [!] File not found: {file_name}')

        
    def recv_result(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel_result.start_consuming()