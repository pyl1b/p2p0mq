# -*- coding: utf-8 -*-
"""
Unit tests for Receiver.
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
import shutil
import tempfile
import uuid
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock

import zmq

from p2p0mq.app.server import Receiver
from p2p0mq.app.theapp import TheApp
from p2p0mq.constants import MESSAGE_TYPE_ROUTE, MESSAGE_TYPE_REQUEST, MESSAGE_TYPE_REPLY
from p2p0mq.message_queue.slow import SlowMessageQueue

logger = logging.getLogger('tests.p2p0mq.server')


class TestTestee(TestCase):
    def setUp(self):
        self.app = MagicMock(spec=TheApp)
        self.app._uuid = uuid.uuid4().hex.encode()
        self.app.uuid = self.app._uuid

        self.testee = Receiver(
            app=self.app,
            bind_address=None
        )

    def tearDown(self):
        self.testee.context.destroy()
        self.testee = None

    def test_init(self):
        self.testee = Receiver(
            app=None,
            bind_address='1'
        )
        self.assertEqual(self.testee.bind_address, '1')
        self.assertEqual(self.testee.name, 'p2p0mq.S.th')
        self.assertIsNone(self.testee.bind_port)
        self.assertIsNone(self.testee.app)
        self.assertIsNotNone(self.testee.context)
        self.assertIsNone(self.testee.socket)
        self.assertFalse(self.testee.no_encryption)
        self.assertEquals(len(self.testee.typed_queues), 3)
        self.assertIsInstance(self.testee.typed_queues[MESSAGE_TYPE_REPLY],
                              SlowMessageQueue)
        self.assertIsInstance(self.testee.typed_queues[MESSAGE_TYPE_REQUEST],
                              SlowMessageQueue)
        self.assertIsInstance(self.testee.typed_queues[MESSAGE_TYPE_ROUTE],
                              SlowMessageQueue)
        self.assertEqual(self.testee.name, 'p2p0mq.S.th')

        app = MagicMock()
        app.uuid = None
        self.testee = Receiver(
            app=app,
            bind_address='1'
        )
        self.assertEqual(self.testee.name, 'p2p0mq.S.th')

        app = MagicMock()
        app.uuid = b'654321'
        self.testee = Receiver(
            app=app,
            bind_address='1'
        )
        self.assertEqual(self.testee.name, '4321-p2p0mq.S.th')

    def test_create(self):
        self.testee.app.no_encryption = True
        with self.assertRaises(zmq.error.ZMQError):
            self.testee.bind_address = "--/--"
            self.testee.create()
        self.testee.bind_address = "tcp://127.0.0.1:19998"
        self.testee.create()
        self.assertIsInstance(self.testee.socket, zmq.Socket)
        self.assertEqual(self.testee.socket.getsockopt(zmq.LINGER), 0)

    def test_enqueue(self):
        raise SkipTest
