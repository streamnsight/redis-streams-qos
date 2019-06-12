FROM python:alpine3.9

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY merger.py /app

ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV PYTHONUNBUFFERED=0

CMD /app/merger.py --redis-server $REDIS_HOST --redis-port $REDIS_PORT