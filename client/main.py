import multiprocessing
from client import Client

if __name__ == "__main__":
    client = Client()
    send_books_process = multiprocessing.Process(target=client.send_books_data())
    send_books_process.run()

    send_rating_process = multiprocessing.Process(target=client.send_rating_data())
    send_rating_process.run()

    recv_result_process = multiprocessing.Process(target=client.recv_result())
    recv_result_process.run()

    send_books_process.join()
    send_rating_process.join()
    recv_result_process.join()