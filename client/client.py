import os
import logging
import socket
import time
from common.log import init_log
from common.protocol import QueryType


class Client:
    def __init__(self):
        init_log()
        self.config = self.__init_config()
        time.sleep(10)
        self.received_results = 0 

        self.queue_manager = QueueManager()
        self.books_data = 'books_data_1' if str(self.config["query_type"]) == QueryType.QUERY2.value else 'books_data'


        # Queue to send books_data
        self.queue_manager.setup_send_queue(self.books_data)
        # Queue to send rating_data
        self.queue_manager.setup_send_queue('rating_data')

        self.queue_manager_2 = QueueManager()
        # Queue to receive result
        self.queue_manager_2.setup_receive_queue('result', self.__process_result)



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
        query_type = str(self.config["query_type"])

        if os.path.isfile(file_name):
            with open(file_name, 'r', encoding='utf-8') as file:
                # Discard header
                reader = csv.reader(file)
                next(reader)

                if query_type == QueryType.QUERY1 or query_type == QueryType.QUERY3:
                    logging.info(f"{query_type}")
                    self.queue_manager.send_message('books_data', "Query3")
                #if query_type == QueryType.QUERY1.value:
                #self.queue_manager.send_message('books_data', query_type)

                for line in reader:
                    msg = self.__filter_book_data_line(line)
                    self.queue_manager.send_message(self.books_data, msg)

                self.queue_manager.send_message(self.books_data, "END") 

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
        finished = False
        while not finished:
            try:
                logging.info(' [*] Waiting for messages. To exit press CTRL+C')
                self.queue_manager_2.start_consuming('result')
                finished = True
            except:
                logging.info("Failed to receive result")
