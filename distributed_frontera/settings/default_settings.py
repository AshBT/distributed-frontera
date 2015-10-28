# -*- coding: utf-8 -*-
from frontera.settings.default_settings import *

OVERUSED_SLOT_FACTOR = 2.0
DELAY_ON_EMPTY = 30.0
URL_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.hostname_local_fingerprint'

MESSAGE_BUS = 'distributed_frontera.messagebus.zeromq.MessageBus'

HBASE_THRIFT_HOST = 'localhost'
HBASE_THRIFT_PORT = 9090
HBASE_NAMESPACE = 'crawler'
HBASE_DROP_ALL_TABLES = False
HBASE_QUEUE_PARTITIONS = 4
HBASE_METADATA_TABLE = 'metadata'
HBASE_USE_SNAPPY = False
HBASE_USE_COMPACT_PROTOCOL = False
HBASE_BATCH_SIZE = 9216
HBASE_STORE_CONTENT = False
HBASE_STATE_CACHE_SIZE_LIMIT = 3000000
HBASE_QUEUE_TABLE = 'queue'

