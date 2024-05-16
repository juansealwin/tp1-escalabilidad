FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY server /
COPY common /common
COPY rabbitmq /rabbitmq

ENTRYPOINT ["python3", "main.py"]