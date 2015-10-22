# -*- coding: utf-8 -*-
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
import sys

def main():
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    us = mb.update_score()
    consumer_sl = sl.consumer(partition_id=partition_id, type='db')
    consumer_us = us.consumer()
    while True:
        for m in consumer_sl.get_messages(timeout=1.0):
            print "sl:", m
        for m in consumer_us.get_messages(timeout=1.0):
            print "us:", m
        sys.stdout.write('.')


if __name__ == '__main__':
    main()