import multiprocessing
from client import Client

if __name__ == "__main__":
    client = Client()

    send_books_process = multiprocessing.Process(target=client.send_books_data)
    send_books_process.start()

    send_rating_process = multiprocessing.Process(target=client.send_rating_data)
    send_rating_process.start()

    recv_result_process = multiprocessing.Process(target=client.recv_result)
    recv_result_process.start()

    send_books_process.join()
    send_rating_process.join()
    recv_result_process.join()
