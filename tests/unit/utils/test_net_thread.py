# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch

from p2p0mq.utils.thread.netthread import KoNetThread, ThreadAuthenticator

logger = logging.getLogger('tests.p2p0mq.thread')


class TestKoNetThread(TestCase):
    def setUp(self):
        self.testee = KoNetThread(
            app=MagicMock(),
            bind_address=MagicMock(),
            bind_port=MagicMock(),
            context=MagicMock(),
            no_encryption=MagicMock()
        )

    def tearDown(self):
        # self.testee.context.destroy()
        pass

    def test_init(self):
        with self.assertRaises(ValueError):
            KoNetThread(
                app=None,
                bind_address="",
                bind_port="bind_port")

        self.testee = KoNetThread(
            app="app",
            bind_address="bind_address",
            bind_port=10,
            context="context",
            no_encryption="no_encryption"
        )
        self.assertEqual(self.testee.app, "app")
        self.assertEqual(self.testee.bind_address, "bind_address")
        self.assertEqual(self.testee.bind_port, 10)
        self.assertEqual(self.testee.context, "context")
        self.assertEqual(self.testee.no_encryption, "no_encryption")
        self.assertIsInstance(self.testee.stop, threading.Event)
        self.assertIsInstance(self.testee.sleep, threading.Event)
        self.assertIsNone(self.testee.tick)
        self.assertIsInstance(self.testee, threading.Thread)

    def test_create(self):
        pass

    def test_terminate(self):
        self.assertIsNone(self.testee.socket)
        self.testee.terminate()

        self.testee.socket = MagicMock()
        sk = self.testee.socket
        self.testee.terminate()
        sk.close.assert_called_once()
        self.assertIsNone(self.testee.socket)

    def test_address(self):
        self.testee.bind_address = "1"
        self.testee.bind_port = 1
        self.assertEqual(self.testee.address, "tcp://1:1")
        self.testee.bind_address = "1"
        self.testee.bind_port = None
        self.assertEqual(self.testee.address, "1")

