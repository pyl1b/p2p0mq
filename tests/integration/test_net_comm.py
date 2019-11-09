# -*- coding: utf-8 -*-
"""
Tests the communication between a server and a client.
"""
from __future__ import unicode_literals
from __future__ import print_function

import binascii
import logging
import os
import threading
import uuid
from random import randint
from time import sleep
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock, patch
from time import time

import zmq
from zmq.utils.monitor import recv_monitor_message

from p2p0mq.app.client import Sender
from p2p0mq.app.server import Receiver
from p2p0mq.app.theapp import TheApp

logger = logging.getLogger('tests.p2p0mq.net_comm')
# handler = logging.StreamHandler()
# handler.setLevel(1)
# logger.addHandler(handler)
# logger.setLevel(1)


class TestNetCommNoAuth(TestCase):
    def setUp(self):

        self.app = MagicMock(spec=TheApp)
        self.app.no_encryption = True
        self.app._uuid = uuid.uuid4().hex.encode()
        self.app.uuid = self.app._uuid
        self.app.zmq_context = zmq.Context.instance()

        self.sender = Sender(
            app=self.app,
            bind_port=19996,
            context=self.app.zmq_context
        )
        self.sender.create()
        self.sender.socket.setsockopt(zmq.IDENTITY, b"sender")

        self.receiver = Receiver(
            app=self.app,
            bind_port=19997,
            context=self.app.zmq_context
        )
        self.receiver.create()
        self.receiver.socket.setsockopt(zmq.IDENTITY, b"receiver")
        self.receiver.socket.connect(self.sender.address)

    def tearDown(self) -> None:
        self.sender.terminate()
        self.receiver.terminate()
        if self.sender.socket is not None:
            self.sender.socket.close()
        if self.receiver.socket is not None:
            self.receiver.socket.close()

    def test_one(self):
        poller = zmq.Poller()
        poller.register(self.sender.socket, zmq.POLLIN | zmq.POLLOUT)
        poller.register(self.receiver.socket, zmq.POLLIN | zmq.POLLOUT)

        received_by_receiver = []
        received_by_sender = []

        for i in range(2):
            sleep(0.2)
            socks = dict(poller.poll(200))
            logger.debug('we have %d active sockets', len(socks))

            if self.sender.socket in socks:
                value = socks[self.sender.socket]
                if value & zmq.POLLIN:
                    data = self.sender.socket.recv_multipart()
                    logger.debug(
                        '<<<<<< SSS <<<<<<<< %r', data)
                    received_by_sender.append(data)
                if value & zmq.POLLOUT:
                    if i == 0:
                        to_send = [b"receiver", b'sender ok']
                        self.sender.socket.send_multipart(to_send)
                        logger.debug(
                            '>>>>> SSS >>>>>> %r', to_send)

            if self.receiver.socket in socks:
                value = socks[self.receiver.socket]
                if value & zmq.POLLIN:
                    data = self.receiver.socket.recv_multipart()
                    logger.debug(
                        '<<<<<< RRR <<<<<<<< %r', data)
                    received_by_receiver.append(data)
                if value & zmq.POLLOUT:
                    if i == 0:
                        to_send = [b"sender", b'receiver ok']
                        self.receiver.socket.send_multipart(to_send)
                        logger.debug(
                            '>>>>> RRR >>>>>> %r', to_send)
        raise SkipTest
        self.assertListEqual(
            received_by_receiver, [[b'sender ok']])
        self.assertListEqual(
            received_by_sender, [[b'receiver', b'sender', b'receiver ok']])
