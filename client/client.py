import os
import pika
import time
import logging
import csv
import os
from configparser import ConfigParser
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *

class Client:
    def __init__(self):
        init_log()
        self.config = self.__init_config()
        time.sleep(10)

        self.queue_manager = QueueManager()

        # Queue to send books_data
        self.queue_manager.setup_send_queue('books_data')
        # Queue to send rating_data
        self.queue_manager.setup_send_queue('rating_data')

        # Queue to receive result
        self.queue_manager.setup_receive_queue('result', self.__process_result)



    def __init_config(self):
        init_log()

        """ Parse env variables or config file to find program config params"""
        config = ConfigParser(os.environ)
    
        config_params = {}
        try:
            config_params["books_data_file"] = os.getenv('BOOKS_DATA_FILE', config["DEFAULT"]["BOOKS_DATA_FILE"])
            config_params["books_rating_file"] = os.getenv('BOOKS_RATING_FILE', config["DEFAULT"]["BOOKS_RATING_FILE"])
            config_params["query_type"] = os.getenv('QUERY_TYPE', config["DEFAULT"]["QUERY_TYPE"])

        except KeyError as e:
            raise KeyError("Key was not found. Error: {} .Aborting client".format(e))

        except ValueError as e:
            raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

        return config_params


    def __process_result(self, ch, method, properties, body):
        logging.info(f" [x] Received {body}")    

        if body.decode('utf-8') == "END":
            logging.info("Received END message. Exiting...")
            os._exit(0)

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
        query_type = self.config["query_type"]

        if os.path.isfile(file_name):
            with open(file_name, 'r', encoding='utf-8') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                if query_type == QueryType.QUERY1:
                    self.queue_manager.send_message('books_data', query_type)

                for line in reader:
                    msg = self.__filter_book_data_line(line)
                    self.queue_manager.send_message('books_data', msg)

                self.queue_manager.send_message('books_data', "END") 

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
                    self.queue_manager.send_message('rating_data', msg)
                self.queue_manager.send_message('rating_data', "END")

        else:
            logging.info(f' [!] File not found: {file_name}')

        
    def recv_result(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('result')
