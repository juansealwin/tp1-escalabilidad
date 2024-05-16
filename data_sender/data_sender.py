import os
import time
import logging
import csv
from configparser import ConfigParser
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *

class DataSender:
    # Books data header: 
    # 'Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount'
    # Discard fileds descripcion[1], image[3], previewLink[4], infoLink[7]
    BOOK_FIELDS_MAP = {
        'Title': 0,
        'Authors': 2,
        'Publisher': 5,
        'PublishedDate': 6,
        'Categories': 8,
        'RatingsCount': 9
    }

    # Ratings data header: 
    # 'Id, Title, Price, User_id, ProfileName, review/helpfulness, review/score, review/time, review/summary, review/text'
    # Discard fileds Id[0], Price[2], ProfileName[4], review/time[7]
    RATING_FIELDS_MAP = {
        'Title': 1,
        'UserId': 3,
        'ReviewHelpfulness': 5,
        'ReviewScore': 6,
        'ReviewSummary': 8,
        'ReviewText': 9
    }

    def __init__(self):
        init_log()
        self.__init_config()
        time.sleep(10)
        self.received_results = 0 

        self.queue_manager = QueueManager()

        # Queue to send data
        self.queue_manager.setup_send_queue(self.queue_name)

        # Queue to receive result
        self.queue_manager.setup_receive_queue('client_request', self.__process_result)

        self.current_query_type = None

    def __init_config(self):
        init_log()

        """ Parse env variables or config file to find program config params"""
        config = ConfigParser(os.environ)
    
        try:
            self.file_name = os.getenv('DATA_FILE', config["DEFAULT"]["DATA_FILE"])
            self.queue_name = os.getenv('QUEUE_NAME', config["DEFAULT"]["QUEUE_NAME"])
            
            if self.queue_name not in ['books_data', 'rating_data']:
                raise ValueError(f"Invalid QUEUE_NAME: {self.queue_name}. QUEUE_NAME name must be 'books_data' or 'rating_data'")

            if self.queue_name == 'books_data':
                self.fields_map = self.BOOK_FIELDS_MAP
            else: 
                self.fields_map = self.RATING_FIELDS_MAP 
            
        except KeyError as e:
            raise KeyError("Key was not found. Error: {} .Aborting DataSender".format(e))

        except ValueError as e:
            raise ValueError("Key could not be parsed. Error: {}. Aborting DataSender".format(e))


    def __filter_data_line(self, line, fields_map):
        filtered_fields = [line[index] for _, index in fields_map.items()]
        filtered_line = '|'.join(filtered_fields)
        return filtered_line


    def __process_result(self, ch, method, properties, body):
        line = body.decode('utf-8')
        logging.info(f"recibi {line}")
        current_query_type = QueryType.validate_query_type(line)
        if current_query_type:
            self.current_query_type = current_query_type
            self.send_data()
            self.current_query_type = None

        else:
            logging.info(f"[x] Received wrong first line: {line}")    


    def send_data(self):

        if os.path.isfile(self.file_name):
            with open(self.file_name, 'r', encoding='utf-8') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                self.queue_manager.send_message(self.queue_name, self.current_query_type)

                for line in reader:
                    msg = self.__filter_data_line(line, self.RATING_FIELDS_MAP)
                    self.queue_manager.send_message(self.queue_name, msg)

                self.queue_manager.send_message(self.queue_name, "END") 
        else:
            logging.info(f' [!] File not found: {self.file_name}')
            

    def recv_request(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('client_request')
