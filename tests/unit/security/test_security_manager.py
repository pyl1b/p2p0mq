# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock, patch
from time import time, sleep
import shutil
import tempfile

import zmq
from zmq.auth.thread import ThreadAuthenticator

from p2p0mq.security import SecurityManager

logger = logging.getLogger('tests.p2p0mq.sec')


class TestKoNetThreadNoDb(TestCase):
    def setUp(self):
        # Create a temporary directory
        self.private_dir = tempfile.mkdtemp()
        self.public_dir = tempfile.mkdtemp()
        self.temp_cert_dir = tempfile.mkdtemp()

        self.testee = SecurityManager(
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
        self.testee = SecurityManager("1", "2", "3")
        self.assertEqual(self.testee.private_cert_dir, "1")
        self.assertEqual(self.testee.public_cert_dir, "2")
        self.assertEqual(self.testee.temp_cert_dir, "3")
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        test_2 = SecurityManager("1", "2")
        self.assertEqual(test_2.temp_cert_dir, tempfile.gettempdir())
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        self.assertIsNone(self.testee.public_file)
        self.assertIsNone(self.testee.private_file)
        self.assertIsNone(self.testee.auth_thread)

    def test_prepare_cert_store(self):
        shutil.rmtree(self.private_dir)
        shutil.rmtree(self.public_dir)
        shutil.rmtree(self.temp_cert_dir)
        self.assertFalse(os.path.isdir(self.private_dir))
        self.assertFalse(os.path.isdir(self.public_dir))
        self.assertFalse(os.path.isdir(self.temp_cert_dir))
        self.testee.prepare_cert_store("1x1")
        self.assertTrue(os.path.isdir(self.private_dir))
        self.assertTrue(os.path.isdir(self.public_dir))
        self.assertTrue(os.path.isdir(self.temp_cert_dir))
        self.assertTrue(os.path.isfile(self.testee.public_file))
        self.assertTrue(os.path.isfile(self.testee.private_file))
        self.assertTrue(os.path.isfile(self.testee.public_file))
        self.assertTrue(os.path.isfile(self.testee.private_file))

    def test_cert_pair_check_gen(self):
        raise SkipTest("lazy")

    def test_cert_file_by_uuid(self):
        result = self.testee.cert_file_by_uuid(uuid="1", public=True)
        self.assertEqual(result, os.path.join(
            self.public_dir, "1.key"))
        result = self.testee.cert_file_by_uuid(uuid="1", public=False)
        self.assertEqual(result, os.path.join(
            self.private_dir, "1.key_secret"))

    def test_start_auth(self):
        with patch('p2p0mq.security.ThreadAuthenticator',
                   autospec=True) as ThAuth:
            self.testee.no_encryption = True
            self.testee.start_auth(zmq.Context.instance())
            ThAuth.assert_not_called()
            ThAuth.return_value.start.assert_not_called()

        with patch('p2p0mq.security.ThreadAuthenticator',
                   autospec=True) as ThAuth:
            self.testee.no_encryption = False
            self.testee.start_auth(zmq.Context.instance())
            ThAuth.assert_called_once()
            ThAuth.return_value.start.assert_called_once()
            self.assertEqual(self.testee.auth_thread.thread.name, 'zmq_auth')

    def test_stop_auth(self):
        self.assertIsNone(self.testee.auth_thread)
        self.testee.auth_thread = MagicMock()
        self.testee.auth_thread = MagicMock()
        at = self.testee.auth_thread
        self.testee.terminate_auth()
        at.stop.assert_called_once()
        self.assertIsNone(self.testee.auth_thread)

    def test_start_stop_auth(self):
        self.testee.no_encryption = False
        self.testee.start_auth(zmq.Context.instance())
        self.assertIsInstance(self.testee.auth_thread, ThreadAuthenticator)
        sleep(0.1)
        self.assertTrue(self.testee.auth_thread.is_alive())
        self.testee.terminate_auth()
        self.assertIsNone(self.testee.auth_thread)
