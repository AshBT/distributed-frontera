# -*- coding: utf-8 -*-
from base import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, BaseSpiderFeedStream, \
    BaseUpdateScoreStream
from distributed_frontera.worker.partitioner import FingerprintPartitioner, Crc32NamePartitioner
from socket_config import SocketConfig
import zmq
from time import time, sleep
from struct import pack, unpack
import six
from logging import getLogger


class Consumer(BaseStreamConsumer):
    def __init__(self, context, location, partition_id, identity):
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(location)

        filter = identity+pack('>B', partition_id) if partition_id is not None else identity
        self.subscriber.setsockopt(zmq.SUBSCRIBE, filter)
        self.counter = None
        self.logger = getLogger("distributed_frontera.messagebus.zeromq.Consumer")

    def get_messages(self, timeout=0.1, count=1):
        started = time()
        while count:
            try:
                msg = self.subscriber.recv_multipart(copy=True, flags=zmq.NOBLOCK)
            except zmq.Again:
                sleep(0.01)
                if time() - started > timeout:
                    break
            else:
                seqno, = unpack(">I", msg[2])
                if not self.counter:
                    self.counter = seqno
                elif self.counter != seqno:
                    self.logger.warning("Sequence counter mismatch: expected %d, got %d. Check if system "
                                        "isn't missing messages." % (self.counter, seqno))
                    self.counter = None
                yield msg[1]
                count -= 1
                if self.counter:
                    self.counter += 1


class Producer(object):
    def __init__(self, context, location, identity):
        self.identity = identity
        self.sender = context.socket(zmq.PUB)
        self.sender.connect(location)
        self.sender.setsockopt(zmq.RCVHWM, 30000)
        self.counter = 0

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        partition = self.partitioner.partition(key)
        for msg in messages:
            self.sender.send_multipart([self.identity+pack(">B", partition), msg, pack(">I", self.counter)])
            self.counter += 1
            if self.counter == 4294967296:
                self.counter = 0

    def flush(self):
        pass


class SpiderLogProducer(Producer):
    def __init__(self, context, location, partitions):
        super(SpiderLogProducer, self).__init__(context, location, 'sl')
        self.partitioner = FingerprintPartitioner(partitions)


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.sw_in_location = messagebus.socket_config.sw_in()
        self.db_in_location = messagebus.socket_config.db_in()
        self.out_location = messagebus.socket_config.spiders_out()
        self.partitions = messagebus.spider_log_partitions

    def producer(self):
        return SpiderLogProducer(self.context, self.out_location, self.partitions)

    def consumer(self, partition_id, type):
        location = self.sw_in_location if type == 'sw' else self.db_in_location
        return Consumer(self.context, location, partition_id, 'sl')


class UpdateScoreProducer(Producer):
    def __init__(self, context, location):
        super(UpdateScoreProducer, self).__init__(context, location, 'us')

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        for msg in messages:
            self.sender.send_multipart([self.identity, msg, pack(">I", self.counter)])
            self.counter += 1
            if self.counter == 4294967296:
                self.counter = 0


class UpdateScoreStream(BaseUpdateScoreStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.sw_out()
        self.out_location = messagebus.socket_config.db_in()

    def consumer(self):
        return Consumer(self.context, self.out_location, None, 'us')

    def producer(self):
        return UpdateScoreProducer(self.context, self.in_location)


class SpiderFeedProducer(Producer):
    def __init__(self, context, location, partitions):
        super(SpiderFeedProducer, self).__init__(context, location, 'sf')
        self.partitioner = Crc32NamePartitioner(partitions)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.db_out()
        self.out_location = messagebus.socket_config.spiders_in()
        self.partitions = messagebus.spider_feed_partitions

    def consumer(self, partition_id):
        return Consumer(self.context, self.out_location, partition_id, 'sf')

    def producer(self):
        return SpiderFeedProducer(self.context, self.in_location, self.partitions)

    def available_partitions(self):
        return self.partitions


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.context = zmq.Context()

        # FIXME: Options!
        self.socket_config = SocketConfig("127.0.0.1", 5550)

        self.spider_log_partitions = [i for i in range(1)]
        self.spider_feed_partitions = [i for i in range(2)]

    def spider_log(self):
        return SpiderLogStream(self)

    def update_score(self):
        return UpdateScoreStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)