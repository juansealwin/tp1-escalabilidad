import pika, time, logging, os
from common.log import init_log

class ColumnFilter:
    TITLE_POS = 0
    PUBLISHER_DATE_POS = 3
    CATEGORY_POS = 4 

    def __init__(self):
        self.__init_config()
        time.sleep(10)
        self.__connect_to_rabbitmq()
        self.__declare_queues()

    def __init_config(self):
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
        

    def __declare_queues(self):
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue='books_analizer_data', durable=True)
        self.__channel.basic_consume(queue='books_analizer_data', on_message_callback=self.__process_message, auto_ack=True)

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
        self.__channel.start_consuming()