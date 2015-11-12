# -*- coding: utf-8 -*-
from distributed_frontera.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
import sys
import logging


def main():
    logging.basicConfig()
    mb = MessageBus(None)
    sl = mb.spider_log()
    us = mb.scoring_log()
    sf = mb.spider_feed()
    consumer_sl = sl.consumer(partition_id=None, type='db')
    consumer_us = us.consumer()
    producer_sf = sf.producer()
    sl_c = 0
    us_c = 0
    while True:
        for m in consumer_sl.get_messages(timeout=0.1, count=512):
            sl_c += 1
        for m in consumer_us.get_messages(timeout=0.1, count=512):
            us_c += 1
        for i in range(0, 256):
            producer_sf.send("newhost", "http://newhost/new/url/to/crawl")
            producer_sf.send("someotherhost", "http://newhost223/new/url/to/crawl")
        #if sl_c % 1000 == 0 or us_c % 1000 == 0:
        print "sl: %d, us: %d" % (sl_c, us_c)


if __name__ == '__main__':
    main()