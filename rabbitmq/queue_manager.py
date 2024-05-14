import pika
import logging
import time

class QueueManager:
    def __init__(self, connection=None, host='rabbitmq', single_channel = False):
        self.host = host
        self.single_channel = single_channel
        self.__connect()
        
        # To be used only in the case of multiple workers
        self.process_result = None
        self.worker_id = None
        self.is_leader = None
        self.total_workers = None
        
        if not single_channel:
            self.channels = {}
        else:
            self.channel = self.connection.channel()


    def __connect(self):
        logging.info(' [*] Waiting for RabbitMQ to start...')
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info(' [!] RabbitMQ not available yet, waiting...')
                time.sleep(2)

        logging.info(' [*] Connected to RabbitMQ')        

    def setup_send_queue(self, queue_name, durable=True):
        if self.single_channel:
            self.channel.queue_declare(queue_name, durable=durable)
        else:
            channel = self.connection.channel()
            channel.queue_declare(queue=queue_name, durable=durable)
            self.channels[queue_name] = channel 

    def setup_receive_queue(self, queue_name, callback, durable=True, auto_ack=False):
        if self.single_channel:
            self.channel.queue_declare(queue_name,durable=durable)
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
        else:
            logging.debug("creating new channel")
            channel = self.connection.channel()
            channel.queue_declare(queue=queue_name, durable=durable)
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
            self.channels[queue_name] = channel

    def send_message(self, queue_name, message):
        if self.single_channel:
            try:
                self.channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            except:
                logging.error(f'send_message: Queue "{queue_name}" not found.')
        else:
            channel = self.channels.get(queue_name)
            if channel:
                channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            else:
                logging.error(f'send_message: Queue "{queue_name}" not found.')

    def start_consuming(self, queue_name):
        channel = self.channels.get(queue_name)
        logging.debug(f'{channel}')
        if channel:
            logging.info(f'start consuming queue "{queue_name}"')
            channel.start_consuming()
        else:
            logging.error(f'start_consuming: Queue "{queue_name}" not found.')
            

    def setup_leader_queues(self, id, leader_id, total_workers, process_result):
       
        self.worker_id = id
        self.is_leader = self.worker_id == leader_id
        self.total_workers = total_workers

        if self.is_leader:
            logging.info("Leader setting up channels")
            self.setup_receive_queue('leader_finish', self.__process_internal_finish_msg)
            
        else:
            logging.info("Setting up finish channel")
            self.setup_send_queue('leader_finish')
            self.setup_receive_queue(f'leader_finish_{self.worker_id}', self.__process_internal_finish_msg)
            
        self.process_result = process_result
    
    def propagate_end_message(self):
        if self.is_leader:
            self.__leader_message(self.worker_id)
            
        else:
            self.send_message('leader_finish', f"END,{self.worker_id}")

        self.process_result()    

    def __process_internal_finish_msg(self, ch, method, properties, body):
        line = body.decode('utf-8')
        fields = line.split(',')
        logging.info(f"Worker_{self.worker_id} finished processing all messages")
        if self.is_leader:
            self.__leader_message(fields[1])
        
        self.process_result()  

    def __leader_message(self, sender):
        logging.info("Letting everyone know")
        finishing_channel = self.connection.channel()
        for i in range(0, self.total_workers):
            if i != self.worker_id and i!= sender:
                queue_name = f'leader_finish_{i}'
                finishing_channel.queue_declare(queue=queue_name, durable=True)
                finishing_channel.basic_publish(exchange='', routing_key=queue_name, body="END")

    def stop_consuming(self, queue_name):
        channel = self.channels.get(queue_name)
        if channel:
            channel.stop_consuming()
        else:
            logging.error(f'start_consuming: Queue "{queue_name}" not found.')

    def start_consuming_sq(self):
        try:
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f'start_consuming_sq: not in sq mode {e}')
    

