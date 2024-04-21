FROM python:3.9.7-slim

COPY . /client

WORKDIR /client

RUN pip install --upgrade pip && pip3 install pika

CMD ["python", "main.py"]