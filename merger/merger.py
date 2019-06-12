#!/usr/local/bin/python -u

import argparse
import redis
from time import sleep
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    args = parser.parse_args()

    client = redis.Redis(port=args.redis_port, host=args.redis_server)

    stream_names = client.keys('stream:*')
    # keep track of message ids, start with 0
    streams = {}
    for s in stream_names:
        streams[s] = '0'


    try:
        # create consumer group for aggregate stream, create stream if it doesn't exist yet
        client.execute_command("XGROUP", 'CREATE', 'job_stream', 'jobs', '0', 'MKSTREAM')
    except Exception as e:
        # group exist
        pass

    while True:

        # check if the job queue is full
        info = client.execute_command("XINFO", 'GROUPS', 'job_stream')
        nb_consumers = info[0][3]
        pending = info[0][5]
        queue_length = client.execute_command("XLEN", 'job_stream')
        while (queue_length - pending) >= nb_consumers:
            # wait for queue to empty before fetcing next jobs, otherwise
            # all jobs get queued to the may queue as they get in.
            info = client.execute_command("XINFO", 'GROUPS', 'job_stream')
            nb_consumers = info[0][3]
            pending = info[0][5]
            queue_length = client.execute_command("XLEN", 'job_stream')
            sleep(0.1)

        # read from all streams 1 element at a time, and feed them into the merged stream
        key_and_ids = stream_names + [streams.get(k) for k in stream_names]
        data = client.execute_command("XREAD", 'COUNT', '1', 'BLOCK', '0', 'STREAMS', *key_and_ids)

        # forward the payload to the aggregate stream
        # keep track of message ids to fetch the next ones
        for row in data:
            streams[row[0]] = row[1][0][0]
            payload = list(sum(row[1][0][1].items(), ()))
            client.execute_command("XADD", 'job_stream', '*', *payload)
            client.execute_command("XDEL", row[0], streams[row[0]])
            print('{}: {}'.format(datetime.now(), *payload))




if __name__ == '__main__':
    main()
