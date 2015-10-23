# -*- coding: utf-8 -*-
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
import sys

def main():
    mb = MessageBus(None)
    sl = mb.spider_log()
    us = mb.update_score()
    sf = mb.spider_feed()
    consumer_sl = sl.consumer(partition_id=None, type='db')
    consumer_us = us.consumer()
    producer_sf = sf.producer()
    while True:
        for m in consumer_sl.get_messages(timeout=1.0):
            print "sl:", m
        for m in consumer_us.get_messages(timeout=1.0):
            print "us:", m
        producer_sf.send("newhost", "http://newhost/new/url/to/crawl")
        sys.stdout.write('.')


if __name__ == '__main__':
    main()