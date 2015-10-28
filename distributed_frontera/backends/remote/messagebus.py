# -*- coding: utf-8 -*-
from distributed_frontera.settings import Settings
from frontera import Backend
from frontera.core import OverusedBuffer
from codecs.msgpack import Encoder, Decoder
from frontera.utils.misc import load_object


class MessageBusBackend(Backend):
    def __init__(self, manager):
        self._manager = manager
        settings = Settings(attributes=manager.settings.attributes)
        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.mb = messagebus(settings)
        self._encoder = Encoder(manager.request_model)
        self._decoder = Decoder(manager.request_model, manager.response_model)
        self.spider_log_producer = self.mb.spider_log().producer()
        spider_feed = self.mb.spider_feed()
        self.partition_id = settings.get('SPIDER_PARTITION_ID')
        self.consumer = spider_feed.consumer(partition_id=self.partition_id)
        self._get_timeout = float(settings.get('KAFKA_GET_TIMEOUT', 5.0))

        self._buffer = OverusedBuffer(self._get_next_requests,
                                      manager.logger.manager.debug)
        self.consumed = 0

    @classmethod
    def from_manager(clas, manager):
        return clas(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.spider_log_producer.flush()

    def add_seeds(self, seeds):
        self.spider_log_producer.send(seeds[0].meta['fingerprint'], self._encoder.encode_add_seeds(seeds))

    def page_crawled(self, response, links):
        self.spider_log_producer.send(response.meta['fingerprint'], self._encoder.encode_page_crawled(response, links))

    def request_error(self, page, error):
        self.spider_log_producer.send(page.meta['fingerprint'], self._encoder.encode_request_error(page, error))

    def _get_next_requests(self, max_n_requests, **kwargs):
        requests = []
        for encoded in self.consumer.get_messages(count=max_n_requests, timeout=self._get_timeout):
            try:
                request = self._decoder.decode_request(encoded)
                requests.append(request)
            except ValueError:
                self._manager.logger.backend.warning("Could not decode message: {0}".format(encoded))
                pass
            finally:
                self.consumed += 1
        self.spider_log_producer.send('1be68ff556fd0bbe5802d1a100850da29f7f15b11',
                                      self._encoder.encode_offset(self.partition_id, self.consumed))
        return requests

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._buffer.get_next_requests(max_n_requests, **kwargs)