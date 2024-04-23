import pika, time

def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

print(' [*] Waiting for RabbitMQ to start...')
while True:
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        break
    except pika.exceptions.AMQPConnectionError:
        print(' [!] RabbitMQ not available yet, waiting...')
        time.sleep(1)

print(' [*] Connected to RabbitMQ')

channel = connection.channel()
channel.queue_declare(queue='books_analizer', durable=True)
#channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='books_analizer', on_message_callback=callback, auto_ack=True)


print(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()

