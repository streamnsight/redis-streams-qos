#!/usr/local/bin/python -u

import argparse
from time import sleep
import redis
import math
from datetime import datetime


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", help="redis server address", default="localhost")
    parser.add_argument("--redis-port", help="redis server port", default=6379, type=int)
    parser.add_argument("--delay", help="delay between messages (ms)", default=1, type=int)
    parser.add_argument("--id", help="producer id", default=1, type=int)

    args = parser.parse_args()

    client = redis.Redis(port=args.redis_port, host=args.redis_server)
    count = 0

    while True:
        # generate 10^ID msg each step
        # producer with ID 0 sends 1 messages
        # producer with ID 2 sends 100 messages
        for i in range(int(math.pow(10.0, args.id))):
            payload = {
                'msg': '{}-{}'.format(count, i),
                'id': args.id
            }
            client.execute_command("XADD", 'stream:{}'.format(args.id), '*', *list(sum(payload.items(), ())))
            print('{}: {}'.format(datetime.now(), payload))
        count += 1
        sleep(args.delay)


if __name__ == '__main__':
    main()
