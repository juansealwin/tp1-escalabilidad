import time
import logging
import socket
import threading
from common.log import init_log
from rabbitmq.queue_manager import *
from common.protocol import QueryType

class Server:
    def __init__(self):
        self.__init_config()
        #self.queue_manager = QueueManager()
        self.client_socket = None
        self.current_query_type = None

        # Queue to send data
        #self.queue_manager.setup_send_queue('server_request')

        # Queue to receive result
        #self.queue_manager.setup_receive_queue('result', self.__process_result)


    def __init_config(self):
        init_log()
        self.server_address = ('localhost', 12345)

    def handle_client_request(self, client_socket):
        self.client_socket = client_socket
        logging.info('Waiting for valid query_type from client...')

        while True:
            query_type = self.client_socket.recv(1024).decode().strip()
            if QueryType.validate_query_type(query_type):
                logging.info(f'Received valid query_type: {query_type} from client')
                self.current_query_type = query_type
                self.send_query_to_queue()
                self.recv_result()
                break

            else:
                logging.info(f'Received invalid query_type: {query_type} from client')

        

    def __process_result(self, ch, method, properties, body):
        line = body.decode('utf-8')

        self.client_socket.sendall(result.encode())

        if line == "END":
            logging.info('Received END message from result queue. Closing connection...')
            self.client_socket.close()
            
        else:
            logging.info(f'Sending result to client: {result}')
            self.client_socket.sendall(result.encode())

       

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.server_address)
        server_socket.listen(5)
        logging.info('Server is listening for incoming connections...')

        try:
            self.client_socket, _ = server_socket.accept()
            logging.info(f'Connection established with {self.client_socket.getpeername()}')
            client_handler = threading.Thread(target=self.handle_client_request, args=(self.client_socket,))
            client_handler.start()
            result_handler = threading.Thread(target=self.__process_result)
            result_handler.start()
        
        except KeyboardInterrupt:
            logging.info('Server shutting down...')

        finally:
            server_socket.close()


    # def send_query_to_queue(self):
    #     logging.info(f'Sending query_type to server_request queue: {self.current_query_type}')
    #     self.queue_manager.send_message('server_request', self.current_query_type)


    # def recv_result(self):
    #     logging.info(' [*] Waiting for messages. To exit press CTRL+C')
    #     self.queue_manager.start_consuming('result')        