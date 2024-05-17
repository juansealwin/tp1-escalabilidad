import pika, time, logging, os
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *


class ColumnFilter:
    TITLE_POS = 0
    PUBLISHER_DATE_POS = 3
    CATEGORY_POS = 4
    AUTHOR_POS = 1

    def __init__(self):
        init_log()  

        self.queue_manager = QueueManager()

        self.__setup_queues()

        self.current_query_type = None


    def __setup_queues(self):
        # Queue to receive books_data
        self.queue_manager.setup_receive_queue('books_data', self.__process_message)

        # Queue to send book_joiner
        self.queue_manager.setup_send_queue('book_joiner', durable=True)
        # Queue to send fiction_books
        self.queue_manager.setup_send_queue('fiction_books', durable=True)

        # Queue to send result Query1
        self.queue_manager.setup_send_queue('result')
        
           

    def __process_message(self, ch, method, properties, body):
        # Decode the msg
        line = body.decode('utf-8')
        
        if self.current_query_type is None:
            self.current_query_type = QueryType.validate_query_type(line)
            if self.current_query_type == QueryType.QUERY3.value or self.current_query_type == QueryType.QUERY4.value:
                self.queue_manager.send_message('book_joiner',self.current_query_type)

        elif line == "END":
            if self.current_query_type == QueryType.QUERY1.value:
                self.queue_manager.send_message('result', "END")
            
            elif self.current_query_type == QueryType.QUERY5.value:
                self.queue_manager.send_message('fiction_books', "END")
            else: 
                self.queue_manager.send_message('book_joiner', "END")
            ##self.queue_manager.send_message('result', "END")

        else: 
            fields = line.split('|')
            if self.current_query_type == QueryType.QUERY1.value:
                self.__process_message_query1(fields)
            elif self.current_query_type == QueryType.QUERY3.value:
                self.__process_message_query3_4(fields)
            elif self.current_query_type == QueryType.QUERY4.value:
                self.__process_message_query3_4(fields)
            elif self.current_query_type == QueryType.QUERY5.value:
                self.__process_message_query5(fields)                
            else:
                logging.error("[x] column_filter query not implemented")
            
    
    def __process_message_query1(self, fields):

        #Check category
        if not 'Computers' in fields[self.CATEGORY_POS]:
            #logging.info(f" [x] category {fields}")
            return

        #Check publisher date
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
            result_line = ','.join(fields)
            self.queue_manager.send_message('result', result_line)

    def __process_message_query3_4(self, fields):
        try:
            publisher_date_field = fields[self.PUBLISHER_DATE_POS]
        except:
            return
            pass
        try:
            year = int(publisher_date_field)
            if not 1990 <= year <= 1999:
                return
        except:
            # Format #yyyy-mm-dd
            parts = publisher_date_field.split('-')
            if len(parts) > 0:
                try:
                    year = int(parts[0])
                    if not 1990 <= year <= 1999:
                        return

                except ValueError:
                    return
        self.queue_manager.send_message('book_joiner', F"{fields[self.TITLE_POS]}|{fields[self.AUTHOR_POS]}")
        return

    def __process_message_query5(self, fields):
        
        if 'Fiction' in fields[self.CATEGORY_POS]:
            #logging.info(f" [x] column_filter sending: {fields[self.TITLE_POS]}")
            self.queue_manager.send_message('fiction_books', f"{fields[self.TITLE_POS]}")
            
        

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('books_data')