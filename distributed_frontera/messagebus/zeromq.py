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
    def __init__(self, context, location, partition_id, identity, seq_warnings=False):
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(location)

        filter = identity+pack('>B', partition_id) if partition_id is not None else identity
        self.subscriber.setsockopt(zmq.SUBSCRIBE, filter)
        self.counter = 0
        self.count_global = partition_id is None
        self.logger = getLogger("distributed_frontera.messagebus.zeromq.Consumer(%s-%s)" % (identity, partition_id))
        self.seq_warnings = seq_warnings

    def get_messages(self, timeout=0.1, count=1):
        started = time()
        sleep_time = timeout / 10.0
        while count:
            try:
                msg = self.subscriber.recv_multipart(copy=True, flags=zmq.NOBLOCK)
            except zmq.Again:
                if time() - started > timeout:
                    break
                sleep(sleep_time)
            else:
                partition_seqno, global_seqno = unpack(">II", msg[2])
                seqno = global_seqno if self.count_global else partition_seqno
                if not self.counter:
                    self.counter = seqno
                elif self.counter != seqno:
                    if self.seq_warnings:
                        self.logger.warning("Sequence counter mismatch: expected %d, got %d. Check if system "
                                            "isn't missing messages." % (self.counter, seqno))
                    self.counter = None
                yield msg[1]
                count -= 1
                if self.counter:
                    self.counter += 1

    def get_offset(self):
        return self.counter


class Producer(object):
    def __init__(self, context, location, identity):
        self.identity = identity
        self.sender = context.socket(zmq.PUB)
        self.sender.connect(location)
        self.counters = {}
        self.global_counter = 0

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        partition = self.partitioner.partition(key)
        counter = self.counters.get(partition, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity+pack(">B", partition), msg,
                                        pack(">II", counter, self.global_counter)])
            counter += 1
            self.global_counter += 1
            if counter == 4294967296:
                counter = 0
            if self.global_counter == 4294967296:
                self.global_counter = 0
        self.counters[partition] = counter

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
        counter = self.counters.get(0, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity, msg, pack(">II", counter, counter)])
            counter += 1
            if counter == 4294967296:
                counter = 0
        self.counters[0] = counter


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
        self.ready_partitions = set(self.partitions)

    def consumer(self, partition_id):
        return Consumer(self.context, self.out_location, partition_id, 'sf', seq_warnings=True)

    def producer(self):
        return SpiderFeedProducer(self.context, self.in_location, self.partitions)

    def available_partitions(self):
        return self.ready_partitions


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.context = zmq.Context()
        self.socket_config = SocketConfig(settings.get('ZMQ_HOSTNAME'), settings.get('ZMQ_BASE_PORT'))
        self.spider_log_partitions = [i for i in range(settings.get('SPIDER_LOG_PARTITIONS'))]
        self.spider_feed_partitions = [i for i in range(settings.get('SPIDER_FEED_PARTITIONS'))]

    def spider_log(self):
        return SpiderLogStream(self)

    def update_score(self):
        return UpdateScoreStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)