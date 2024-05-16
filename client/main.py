from client import Client
from common.protocol import QueryType

if __name__ == "__main__":
    client = Client()
    client.send_query_to_queue()
    client.recv_result()
    