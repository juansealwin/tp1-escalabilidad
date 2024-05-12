FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY decade_filter /
COPY common /common
COPY rabbitmq /rabbitmq

CMD ["python", "./main.py"]