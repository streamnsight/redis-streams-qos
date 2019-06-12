#!/usr/local/bin/python -u

import argparse
import redis
from time import sleep
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    parser.add_argument("--id", help="consumer id", default=1, type=int)
    args = parser.parse_args()

    client = redis.Redis(port=args.redis_port, host=args.redis_server)

    try:
        # create consumer group for aggregate stream
        client.execute_command("XGROUP", 'CREATE', 'job_stream', 'jobs', '0', 'MKSTREAM')
    except Exception as e:
        print(str(e))
        # group exist
        pass

    idx = '0'

    while True:

        data = client.execute_command("XREADGROUP", 'GROUP', 'jobs', args.id, 'COUNT', '1', 'BLOCK', '100', 'STREAMS',
                                      'job_stream', idx)
        # keep track of message ids to fetch the next ones
        # here we should have 1 row at a time
        if len(data) > 0 and len(data[0][1]) > 0:
            id = data[0][1][0][0]
            payload = list(sum(data[0][1][0][1].items(), ()))
            idx = '>'
        else:
            idx = '>'
            continue

        # process the data
        print('{}: {}'.format(datetime.now(), payload))
        # simulate worker work
        sleep(0.03)

        # ack the message
        client.execute_command("XACK", 'job_stream', 'jobs', id)
        # delete message from stream to free up the queue
        client.execute_command("XDEL", 'job_stream', id)


if __name__ == '__main__':
    main()
