#!/usr/local/bin/python -u

import argparse
import redis
from redis.exceptions import ResponseError
from time import sleep

import logging

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(name)s]%(message)s', level=logging.INFO)
global log

STREAM_NAME = 'job_stream'
CONSUMER_GROUP = 'jobs'


def do_work(id, payload):
    log.info('{}: {}'.format(id, payload))
    # simulate worker work
    sleep(0.03)


def get_payload(data):
    id = data[0][1][0][0]
    payload = list(sum(data[0][1][0][1].items(), ()))
    return id, payload


def loop(args):

    client = redis.Redis(port=args.redis_port, host=args.redis_server)

    try:
        # create consumer group for aggregate stream, starting at the beginning index (0)
        client.execute_command("XGROUP", 'CREATE', STREAM_NAME, CONSUMER_GROUP, '0', 'MKSTREAM')
    except ResponseError:
        # group exist
        pass

    client_id = args.id
    idx = '0'
    recovery = True

    while True:
        # read messages from last id read, or from beginning of stream if nothing read before.
        cmd_args = ["XREADGROUP",
                    'GROUP', CONSUMER_GROUP, client_id,
                    'COUNT', '1',
                    'BLOCK', 5000,
                    'STREAMS', STREAM_NAME, idx]
        data = client.execute_command(*cmd_args)
        if not data:
            sleep(0.03)
            log.info('.')
            continue

        if recovery:
            if data[0][1]:
                log.info(f'{client_id}: Recovering pending messages...')
            else:
                # If there are no messages to recover, switch to fetching new messages
                # and call again.
                recovery = False
                idx = '>'
                continue

        if data[0][1]:
            # get payload
            last_id, payload = get_payload(data)
            # process the data
            do_work(last_id, payload)

            pipeline = client.pipeline()
            # ack the message
            pipeline.execute_command("XACK", STREAM_NAME, CONSUMER_GROUP, last_id)
            # delete message from stream to free up the queue
            pipeline.execute_command("XDEL", STREAM_NAME, last_id)
            pipeline.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    parser.add_argument("--id", help="consumer id", default=1, type=int)
    args = parser.parse_args()
    log = logging.getLogger(f'consumer-{args.id}')

    loop(args)
