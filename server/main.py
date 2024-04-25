import pika, time, logging

time.sleep(10)

def callback(ch, method, properties, body):
    logging.info(f" [x] Received {body}")

logging.basicConfig(level="DEBUG")    

logging.info(' [*] Waiting for RabbitMQ to start...')
while True:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        break
    except pika.exceptions.AMQPConnectionError:
        logging.info(' [!] RabbitMQ not available yet, waiting...')
        time.sleep(1)

logging.info(' [*] Connected to RabbitMQ')
channel = connection.channel()
channel.queue_declare(queue='books_analizer', durable=True)
#channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='books_analizer', on_message_callback=callback, auto_ack=True)


logging.info(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()

