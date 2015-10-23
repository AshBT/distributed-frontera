# -*- coding: utf-8 -*-
import sys
from distributed_frontera.messagebus.zeromq import MessageBus


def main():
    partition_id = int(sys.argv[1])
    mb = MessageBus(None)
    sl = mb.spider_log()
    us = mb.update_score()
    consumer = sl.consumer(partition_id=partition_id, type='sw')
    producer = us.producer()
    while True:
        for m in consumer.get_messages(timeout=1.0):
            print m
        producer.send(None, 'message'+str(partition_id))
        sys.stdout.write('.')


if __name__ == '__main__':
    main()