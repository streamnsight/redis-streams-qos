#!/usr/local/bin/python -u

import argparse
from time import sleep
import redis
import math
from datetime import datetime
import logging

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(name)s]%(message)s', level=logging.INFO)
global log

STREAM_REGISTRY_KEY = 'stream_names'
STREAMS_PREFIX = 'stream'
MAX_STREAM_LENGTH = 10000


def loop(args):
    producer_id = args.id

    client = redis.Redis(port=args.redis_port, host=args.redis_server)

    stream_name = f'{STREAMS_PREFIX}:{producer_id}'
    # check if the stream key exists
    exists = client.hexists(STREAM_REGISTRY_KEY, stream_name)
    # if not, set the initial mesage id to scan to '0'
    if not exists:
        client.hset(STREAM_REGISTRY_KEY, stream_name, '0')
    count = 0

    while True:
        # generate 10^ID msg each step
        # producer with ID 0 sends 1 messages
        # producer with ID 2 sends 100 messages
        for i in range(int(math.pow(10.0, args.id))):
            payload = {
                'msg': f'{count}-{i}',
                'id': args.id
            }
            client.execute_command("XADD", stream_name, 'MAXLEN', MAX_STREAM_LENGTH, '*', *list(sum(payload.items(), ())))
            log.info(payload)
        count += 1
        sleep(args.delay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    parser.add_argument("--delay", help="delay between messages (ms)", default=1, type=int)
    parser.add_argument("--id", help="producer id", default=1, type=int)

    args = parser.parse_args()
    log = logging.getLogger(f'producer-{args.id}')
    loop(args)
