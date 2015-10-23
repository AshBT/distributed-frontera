# -*- coding: utf-8 -*-

from time import time
import logging

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream
from socket_config import SocketConfig
from struct import unpack, pack
from binascii import hexlify

PORT = 5550
BIND_HOSTNAME = '127.0.0.1'


class Server(object):

    ctx = None
    loop = None
    stats = None
    spiders_in = None
    spiders_out = None
    sw_in = None
    sw_out = None
    db_in = None
    db_out = None

    def __init__(self):
        self.ctx = zmq.Context()
        self.loop = IOLoop.instance()
        self.stats = {
            'started': time()
        }

        socket_config = SocketConfig(BIND_HOSTNAME, PORT)

        spiders_in_s = self.ctx.socket(zmq.XPUB)
        spiders_out_s = self.ctx.socket(zmq.XSUB)
        sw_in_s = self.ctx.socket(zmq.XPUB)
        sw_out_s = self.ctx.socket(zmq.XSUB)
        db_in_s = self.ctx.socket(zmq.XPUB)
        db_out_s = self.ctx.socket(zmq.XSUB)

        spiders_in_s.bind(socket_config.spiders_in())
        spiders_out_s.bind(socket_config.spiders_out())
        sw_in_s.bind(socket_config.sw_in())
        sw_out_s.bind(socket_config.sw_out())
        db_in_s.bind(socket_config.db_in())
        db_out_s.bind(socket_config.db_out())

        self.spiders_in = ZMQStream(spiders_in_s)
        self.spiders_out = ZMQStream(spiders_out_s)
        self.sw_in = ZMQStream(sw_in_s)
        self.sw_out = ZMQStream(sw_out_s)
        self.db_in = ZMQStream(db_in_s)
        self.db_out = ZMQStream(db_out_s)

        self.spiders_out.on_recv(self.handle_spiders_out_recv)
        self.sw_out.on_recv(self.handle_sw_out_recv)
        self.db_out.on_recv(self.handle_db_out_recv)

        self.sw_in.on_recv(self.handle_sw_in_recv)
        self.db_in.on_recv(self.handle_db_in_recv)
        self.spiders_in.on_recv(self.handle_spiders_in_recv)
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.INFO)

    def start(self):
        print "Started"
        try:
            self.loop.start()
        except KeyboardInterrupt:
            pass

    def handle_spiders_out_recv(self, msg):
        self.sw_in.send_multipart(msg)
        self.db_in.send_multipart(msg)
        print "SO: ", msg

    def handle_sw_out_recv(self, msg):
        self.db_in.send_multipart(msg)
        print "SWO: ", msg

    def handle_db_out_recv(self, msg):
        self.spiders_in.send_multipart(msg)
        print "DBO: ", msg

    def handle_db_in_recv(self, msg):
        if msg[0][0] in ['\x01', '\x00']:
            action, identity, partition_id = self.decode_subscription(msg[0])
            if identity == 'sl':
                self.spiders_out.send_multipart(msg)
                return
            if identity == 'us':
                self.sw_out.send_multipart(msg)
                return
            raise AttributeError('Unknown identity in channel subscription.')

    def handle_sw_in_recv(self, msg):
        if msg[0][0] in ['\x01', '\x00']:
            self.spiders_out.send_multipart(msg)

    def handle_spiders_in_recv(self, msg):
        if msg[0][0] in ['\x01', '\x00']:
            self.db_out.send_multipart(msg)

    def decode_subscription(self, msg):
        """

        :param msg:
        :return: tuple of action, identity, partition_id
        where
        action is 1 - subscription, 0 - unsubscription,
        identity - 2 characters,
        partition_id - 8 bit unsigned integer (None if absent)
        """
        if len(msg) == 4:
            return unpack(">B2sB", msg)
        elif len(msg) == 3:
            action, identity = unpack(">B2s", msg)
            return action, identity, None
        raise ValueError("Can't decode subscription correctly.")


def main():
    server = Server()
    server.start()

if __name__ == '__main__':
    main()
