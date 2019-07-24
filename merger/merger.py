#!/usr/local/bin/python -u

import argparse
import redis
from redis.exceptions import ResponseError
from time import sleep
from datetime import datetime, timedelta
import logging

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(name)s]%(message)s', level=logging.INFO)
log = logging.getLogger('merger')

STREAM_NAME = 'job_stream'
CONSUMER_GROUP = 'jobs'
STREAMS_REGISTRY_KEY = 'stream_names'
MAX_IDLE_TIME = 60000


def get_msg(row):
    stream_name = row[0]
    msg_id = row[1][0][0]
    payload = list(sum(row[1][0][1].items(), ()))
    return stream_name, msg_id, payload


def get_queue_status(client):
    pipeline = client.pipeline()
    pipeline.execute_command("XINFO", 'GROUPS', STREAM_NAME)
    pipeline.execute_command("XLEN", STREAM_NAME)
    info = pipeline.execute()
    nb_consumers = info[0][0][3]
    pending = info[0][0][5]
    queue_length = info[1]
    return nb_consumers, queue_length, pending


def check_consumer_status(client):
    consumers_info = client.execute_command("XINFO", 'CONSUMERS', STREAM_NAME, CONSUMER_GROUP)
    active_consumers = set()
    stale_consumers = set()
    for consumer in consumers_info:
        _, name, _, pending, _, idle = consumer
        if idle > MAX_IDLE_TIME:
            stale_consumers.add(name)
        else:
            active_consumers.add(name)
    # if we have any active consumer, claim messages from stake consumers
    for consumer in stale_consumers:
        msgs = client.xpending_range(STREAM_NAME, CONSUMER_GROUP, min='-', max='+', count=1000, consumername=consumer)
        if len(msgs) > 0 and len(active_consumers) > 0:
            client.xclaim(STREAM_NAME, CONSUMER_GROUP, list(active_consumers)[0], min_idle_time=MAX_IDLE_TIME, message_ids=[t['message_id'] for t in msgs])
        # if there are other active consumers, messages were claimed
        if len(active_consumers) > 0:
            client.xgroup_delconsumer(STREAM_NAME, CONSUMER_GROUP, consumer)


def loop(args):

    client = redis.Redis(port=args.redis_port, host=args.redis_server)

    # keep track of message ids, start with 0
    last_stream_check = datetime.utcnow()

    try:
        # create consumer group for aggregate stream, create stream if it doesn't exist yet
        client.execute_command("XGROUP", 'CREATE', STREAM_NAME, CONSUMER_GROUP, '0', 'MKSTREAM')
    except ResponseError:
        # group exist
        pass

    while True:
        streams = client.hgetall(STREAMS_REGISTRY_KEY)
        # refresh streams list to pull from every X seconds
        if datetime.utcnow() - last_stream_check > timedelta(seconds=5):
            last_stream_check = datetime.utcnow()
            check_consumer_status(client)

        # check if there are consumers
        nb_consumers, queue_length, pending = get_queue_status(client)
        if nb_consumers == 0:
            # if not, maybe need some sort of warning, spin a consumer or something.
            sleep(0.1)
            continue

        # check if the job queue is full
        while (queue_length - pending) >= nb_consumers:
            # wait for queue to empty before fetching next jobs, otherwise
            # all jobs get queued to the main queue as they get in their respective streams.
            nb_consumers, queue_length, pending = get_queue_status(client)
            sleep(0.1)

        # read from all streams 1 element at a time, and feed them into the merged stream
        key_and_ids = list(streams.keys()) + [streams.get(k) for k in streams.keys()]
        data = client.execute_command("XREAD", 'COUNT', '1', 'BLOCK', 5000, 'STREAMS', *key_and_ids)

        if not data:
            continue

        # forward the payload to the aggregate stream
        # keep track of message ids to fetch the next ones
        for row in data:
            stream_name, msg_id, payload = get_msg(row)
            streams[stream_name] = msg_id
            pipeline = client.pipeline()
            # forward message payload to aggregate stream
            pipeline.execute_command("XADD", STREAM_NAME, '*', *payload)
            # register the last fetched id for the stream
            pipeline.hset(STREAMS_REGISTRY_KEY, stream_name, msg_id)
            # delete the message from original stream
            pipeline.execute_command("XDEL", stream_name, msg_id)
            pipeline.execute()
            log.info(payload)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    args = parser.parse_args()
    loop(args)
