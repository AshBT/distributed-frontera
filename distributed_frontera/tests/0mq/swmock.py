# -*- coding: utf-8 -*-
import sys
from distributed_frontera.messagebus.zeromq import MessageBus
import logging


def main():
    logging.basicConfig()
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    us = mb.scoring_log()
    consumer = sl.consumer(partition_id=partition_id, type='sw')
    producer = us.producer()
    c = 0
    while True:
        for m in consumer.get_messages(timeout=0.1, count=512):
            c += 1
            producer.send(None, 'message'+str(partition_id)+","+str(c))

        print c


if __name__ == '__main__':
    main()