import pika, time, logging, os
from common.log import init_log
from rabbitmq.rabbit_connection import *
from rabbitmq.rabbit_queue import *

class ColumnFilter:
    TITLE_POS = 0
    PUBLISHER_DATE_POS = 3
    CATEGORY_POS = 4 

    def __init__(self):
        self.__init_config()
        time.sleep(10)

        self.__rabbit_conn = RabbitConnection()
        self.__rabbit_conn.connect()
        
        # Queue to send book data
        self.__book_data_queue = RabbitQueue(self.__rabbit_conn.connection, 'books_data')
        self.__book_data_queue.setup_receive_queue(self.__process_message)

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        init_log(log_level)    
        

    def __process_message(self, ch, method, properties, body):
        logging.debug(f" [x] Received {body}")

        # Decode the msg
        line = body.decode('utf-8')

        fields = line.split(',')

        # Check category
        if not 'Computers' in fields[self.CATEGORY_POS]:
            return

        # Check publisher date
        publisher_date_field = fields[self.PUBLISHER_DATE_POS]
        try:
            year = int(publisher_date_field)
            if not 2000 <= year <= 2023:
                return

        except ValueError:
            # Format #yyyy-mm-dd
            parts = publisher_date_field.split('-')
            if len(parts) > 0:
                try:
                    year = int(parts[0])
                    if not 2000 <= year <= 2023:
                        return

                except ValueError:
                    pass

        # Check title
        if 'distributed' in fields[self.TITLE_POS].lower():
            logging.debug(f"All validations checked: {line}")

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.__book_data_queue.start_consuming()