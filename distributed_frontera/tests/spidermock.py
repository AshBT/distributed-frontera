# -*- coding: utf-8 -*-
import sys
from time import sleep
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
from random import randint


def main():
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    sf = mb.spider_feed()
    producer = sl.producer()
    consumer = sf.consumer(partition_id)

    while True:
        producer.send(sha1(str(randint(1, 1000))), 'http://helloworld.com/way/to/the/sun/'+str(partition_id))
        producer.send(sha1(str(randint(1, 1000))), 'http://way.to.the.sun'+str(partition_id))
        for m in consumer.get_messages(timeout=1.0):
            print m
        sys.stdout.write(".")
        sleep(1)


if __name__ == '__main__':
    main()