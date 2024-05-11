import pika, time, logging, os
from common.log import init_log
from common.protocol import QueryType
from rabbitmq.queue_manager import *


class ColumnFilter:
    TITLE_POS = 0
    PUBLISHER_DATE_POS = 3
    CATEGORY_POS = 4 

    def __init__(self):
        self.__init_config()
        time.sleep(10)

        self.queue_manager = QueueManager()


        # Queue to receive result
        self.queue_manager.setup_receive_queue('books_data', self.__process_message)

        # Queue to send book_data
        self.queue_manager.setup_send_queue('result')

        self.current_processing = None

    def __init_config(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        init_log(log_level)    
        

    def __set_current_processing(self, line):
        for query_type in QueryType:
            if line == query_type.value:
                self.current_processing = query_type
                return
            else:
                logging.info(f"line {line}, query_type {query_type}...")    
    
        logging.info(f"[!] Wrong first message: {line}...")    

    def __process_message(self, ch, method, properties, body):
        logging.info(f" [x] Received {body}")

        # Decode the msg
        line = body.decode('utf-8')

        if self.current_processing is None:
            self.__set_current_processing(line)
            
        # TODO: change for each type of query    
        elif line == "END":
            self.queue_manager.send_message('result', "END")

        else: 
            fields = line.split('|')

            if self.current_processing == QueryType.QUERY1:
                self.__process_message_query1(fields)
            elif self.current_processing == QueryType.QUERY2:
                self.__process_message_query2(fields)
            elif self.current_processing == QueryType.QUERY3:
                self.__process_message_query3(fields)
            else:
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
    def __process_message_query2(fields):
        # try:
        #     publisher_date_field = fields[self.PUBLISHER_DATE_POS]
        #     logging.debug(f"DATE : {publisher_date_field}")
        # except:
        #     return
        #     pass
        # try:
        #     year = int(publisher_date_field)
        #     if not 1990 <= year <= 1999:
        #         return
        # except:
        #     # Format #yyyy-mm-dd
        #     parts = publisher_date_field.split('-')
        #     if len(parts) > 0:
        #         try:
        #             year = int(parts[0])
        #             if not 1990 <= year <= 1999:
        #                 logging.debug("sending to next")
        #                 return

        #         except ValueError:
        #             pass
        
        # self.__send_message(self.__book_joiner, fields[self.TITLE_POS], 'book_joiner')
        return

    def __process_message_query3(fields):
        return


    # def __send_message(self, channel, message, routing_key):
    #     channel.basic_publish(message)
    #     logging.debug(f" [x] Sent '{message}'")

    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.queue_manager.start_consuming('books_data')