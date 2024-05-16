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
        time.sleep(10)

        self.queue_manager = QueueManager()


        # Queue to receive result
        self.queue_manager.setup_receive_queue('books_data', self.__process_message)
        self.queue_manager.setup_send_queue('book_joiner', durable=True)

        # Queue to send book_data
        self.queue_manager.setup_send_queue('result')

        self.current_query_type = QueryType.QUERY3.value
        

    def __set_current_query_type(self, line):
        for query_type in QueryType:
            if line == query_type.value:
                self.current_query_type = query_type.value
                return  
    
        logging.info(f"[!] Wrong first message: {line}...")    

    def __process_message(self, ch, method, properties, body):
        # Decode the msg
        line = body.decode('utf-8')

        if self.current_query_type is None:
            self.__set_current_query_type(line)

            
        # TODO: change for each type of query    
        elif line == "END":
            self.queue_manager.send_message('book_joiner', "END")
            pass
            ##self.queue_manager.send_message('result', "END")

        else: 
            fields = line.split('|')

            if self.current_query_type == QueryType.QUERY1.value:
                self.__process_message_query1(fields)
            elif self.current_query_type == QueryType.QUERY3.value:
                self.__process_message_query3(fields)
            elif self.current_query_type == QueryType.QUERY4.value:
                self.__process_message_query4(fields)
            else:
                # TODO
                logging.info("TODO")
            
    
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
            logging.info(f"Send book: {fields}")
            result_line = ','.join(fields)
            self.queue_manager.send_message('result', result_line)

    # TODO
    def __process_message_query3(self,fields):
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

    def __process_message_query4(fields):
        return


    # def __send_message(self, channel, message, routing_key):
    #     channel.basic_publish(message)
    #     logging.debug(f" [x] Sent '{message}'")

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('books_data')