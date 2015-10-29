# -*- coding: utf-8 -*-
import sys
from time import sleep
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
from random import randint
import logging

def main():
    logging.basicConfig()
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    sf = mb.spider_feed()
    producer = sl.producer()
    consumer = sf.consumer(partition_id)
    c = 0
    while True:
        for i in range(0, 256):
            producer.send(sha1(str(randint(1, 1000))), 'http://helloworld.com/way/to/the/sun/'+str(partition_id))
            producer.send(sha1(str(randint(1, 1000))), 'http://way.to.the.sun'+str(partition_id))
        for m in consumer.get_messages(timeout=0.1, count=512):
            c+=1
        print c


if __name__ == '__main__':
    main()