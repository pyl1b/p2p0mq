# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import os
import logging
import shutil
import tempfile
from unittest import TestCase

from p2p0mq.app.client import Sender
from p2p0mq.app.server import Receiver
from p2p0mq.app.theapp import TheApp

logger = logging.getLogger('tests.p2p0mq.app')


class TestKoNetThreadNoDb(TestCase):
    def setUp(self):
        self.private_dir = tempfile.mkdtemp()
        self.public_dir = tempfile.mkdtemp()
        self.temp_cert_dir = tempfile.mkdtemp()
        self.testee = TheApp(
            config=None,
            private_cert_dir=self.private_dir,
            public_cert_dir=self.public_dir,
            temp_cert_dir=self.temp_cert_dir
        )

    def tearDown(self):
        if os.path.isdir(self.private_dir):
            shutil.rmtree(self.private_dir)
        if os.path.isdir(self.public_dir):
            shutil.rmtree(self.public_dir)
        if os.path.isdir(self.temp_cert_dir):
            shutil.rmtree(self.temp_cert_dir)

    def test_init(self):
        self.testee = TheApp(
            config=None,
            private_cert_dir=self.private_dir,
            public_cert_dir=self.public_dir,
            temp_cert_dir=self.temp_cert_dir
        )
        self.assertIsNone(self.testee.db_file_path)
        self.assertIsInstance(self.testee.peers, dict)
        self.assertEqual(len(self.testee.peers), 0)
        self.assertIsNotNone(self.testee.next_peer_db_sync_time)
        self.assertIsNone(self.testee.uuid)
        self.assertIsNone(self.testee.db_created)
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        self.assertEqual(self.testee.name, 'p2p0mq.A.th')
        self.assertIsNone(self.testee.config)
        self.assertFalse(self.testee.no_encryption)
        self.assertFalse(self.testee.zmq_monitor)
        self.assertIsNotNone(self.testee.zmq_context)

        self.assertIsInstance(self.testee.receiver, Receiver)
        self.assertEqual(self.testee.receiver.app, self.testee)
        self.assertEqual(self.testee.receiver.context, self.testee.zmq_context)
        self.assertEqual(self.testee.receiver.bind_address, '127.0.0.1')
        self.assertEqual(self.testee.receiver.bind_port, 8342)

        self.assertIsInstance(self.testee.sender, Sender)
        self.assertEqual(self.testee.sender.app, self.testee)
        self.assertEqual(self.testee.sender.context, self.testee.zmq_context)
        self.assertEqual(self.testee.sender.bind_address, '127.0.0.1')
        self.assertEqual(self.testee.sender.bind_port, 8341)
