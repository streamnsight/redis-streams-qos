FROM python:alpine3.9

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY consumer.py /app

ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV ID=1
ENV PYTHONUNBUFFERED=0

CMD /app/consumer.py --redis-server $REDIS_HOST --redis-port $REDIS_PORT --id $ID