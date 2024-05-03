import os
import pika
import time
import logging
import csv
import os
from configparser import ConfigParser
from common.log import init_log
from rabbitmq.rabbit_connection import *
from rabbitmq.rabbit_queue import *

class Client:
    def __init__(self):
        self.__init_log()
        self.config = self.__init_config()
        time.sleep(10)
        self.__rabbit_conn = RabbitConnection()
        self.__rabbit_conn.connect()

        # Queue to send book data
        self.__book_data_queue = RabbitQueue(self.__rabbit_conn.connection, 'books_data')
        self.__book_data_queue.setup_send_queue()

        # Queue to send rating data
        self.__rating_data_queue = RabbitQueue(self.__rabbit_conn.connection, 'rating_data')
        self.__rating_data_queue.setup_send_queue()

        # Queue to receive result
        self.__result_queue = RabbitQueue(self.__rabbit_conn.connection, 'result')
        self.__result_queue.setup_receive_queue(self.__process_result)

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

    def __process_result(self, ch, method, properties, body):
        logging.info(f" [x] Received {body}")    

        if body.decode('utf-8') == "END":
            logging.info("Received END message. Exiting...")
            os._exit(0)

    def __send_message(self, channel, message, routing_key):
        channel.basic_publish(message)
        #logging.info(f" [x] Sent '{message}'")


    def __filter_book_data_line(self, line):
        # Books data header: 
        # 'Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount'
        # Discard fileds descripcion[1], image[3], previewLink[4], infoLink[7]
        filtered_fields = [line[0], line[2], line[5], line[6], line[8], line[9]]
        filtered_line = '|'.join(filtered_fields)
        return filtered_line

    def __filter_rating_data_line(self, line):
        # Ratings data header: 
        # 'Id, Title, Price, User_id, ProfileName, review/helpfulness, review/score, review/time, review/summary, review/text'
        # Discard fileds Id[0], Price[2], ProfileName[4], review/time[7]
        filtered_fields = [line[1], line[3], line[5], line[6], line[8], line[9]]
        filtered_line = '|'.join(filtered_fields)
        return filtered_line

    def send_books_data(self):

        file_name = self.config["books_data_file"]

        if os.path.isfile(file_name):
            with open(file_name, 'r', encoding='utf-8') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                # TODO: get from env or file
                self.__book_data_queue.send_message("Query1")

                for line in reader:
                    msg = self.__filter_book_data_line(line)
                    self.__book_data_queue.send_message(msg)

                self.__book_data_queue.send_message("END") 

        else:
            logging.info(f' [!] File not found: {file_name}')


    def send_rating_data(self):
        file_name = self.config["books_rating_file"]

        if os.path.isfile(file_name):
            with open(file_name, 'r', encoding='utf-8') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                for line in reader:
                    msg = self.__filter_rating_data_line(line)
                    self.__rating_data_queue.send_message(msg)
                self.__rating_data_queue.send_message("END")

        else:
            logging.info(f' [!] File not found: {file_name}')

        
    def recv_result(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__result_queue.start_consuming()
