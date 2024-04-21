FROM python:3.9.7-slim

COPY . /server

WORKDIR /server

RUN pip install --upgrade pip && pip3 install pika

CMD ["python", "main.py"]