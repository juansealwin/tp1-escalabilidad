import pika, time, os, logging

filename = 'data/books_data.csv'
logging.basicConfig(level="DEBUG")   

def send_message(channel, message):
    channel.basic_publish(exchange='', routing_key='books_analizer', body=message)
    logging.info(f" [x] Sent '{message}'")

logging.info(' [*] Waiting for RabbitMQ to start...')
while True:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        break
    
    except pika.exceptions.AMQPConnectionError:
        logging.info(' [!] RabbitMQ not available yet, waiting...')
        time.sleep(2)

logging.info(' [*] Connected to RabbitMQ')
channel = connection.channel()
channel.queue_declare(queue='books_analizer', durable=True)

if os.path.isfile(filename):
    with open(filename, 'r') as file:
        for line in file:
            msg = line.strip() 
            send_message(channel, msg)
            time.sleep(1)  

else:
    logging.info(' [!] File not found: data/books_data.csv')


if connection.is_open:
    connection.close()