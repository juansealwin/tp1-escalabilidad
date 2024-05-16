FROM python:3.9.7-slim

RUN pip install --upgrade pip && pip3 install pika && pip3 install textblob
RUN python -m textblob.download_corpora

COPY sentiment_filter /
COPY common /common
COPY rabbitmq /rabbitmq

CMD ["python", "./main.py"]