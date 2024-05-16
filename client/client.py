import os
import logging
import socket
import time
from common.log import init_log
from common.protocol import QueryType


class Client:
    def __init__(self):
        self.__init_config()
        self.server_address = ('localhost', 12345)
        self.total_responses = 0 

    def __init_config(self):
        init_log()

        self.query_type = os.getenv('QUERY_TYPE')
        if not QueryType.validate_query_type(self.query_type):
            raise ValueError(f"Invalid query_type: {self.query_type}")

    def request_query(self):

        while True:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                logging.info("Intentando conectarme....")
                client_socket.connect(self.server_address)
                logging.info("Me conecte")
                client_socket.sendall(self.query_type.encode())
                self.recv_response(client_socket)

            except ConnectionRefusedError:
                logging.error("Connection refused. Make sure the server is running.")
                time.sleep(2)
                continue

            finally:
                client_socket.close()


    def recv_response(self, client_socket):
        logging.info("Waiting for response from server...")
        while True:
            response = client_socket.recv(1024).decode()
            logging.info("Response from server:", response)
            self.total_responses += 1

            if response == "END":
                logging.info(f"Received a total of {self.total_responses} responses. Exiting...")
                break

