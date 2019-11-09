# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch
from time import time

from p2p0mq.utils.thread.koloopthread import KoLoopThread

logger = logging.getLogger('tests.p2p0mq.thread')


class TestKoNetThread(TestCase):
    def setUp(self):
        self.testee = KoLoopThread()

    def test_init(self):
        self.testee = KoLoopThread(name="name")
        self.assertIsInstance(self.testee.stop, threading.Event)
        self.assertIsInstance(self.testee.sleep, threading.Event)
        self.assertIsNone(self.testee.tick)
        self.assertEqual(self.testee.name, "name")

    def test_run_stop_set(self):
        self.testee.create = MagicMock()
        self.testee.terminate = MagicMock()
        self.testee.stop = MagicMock()
        self.testee.execute = MagicMock()
        self.testee.sleep = MagicMock()
        self.testee.stop.is_set.return_value = True
        self.testee.run()
        self.testee.create.assert_called_once()
        self.testee.terminate.assert_called_once()
        self.testee.sleep.wait.assert_called_once()
        self.testee.sleep.clear.assert_called_once()
        self.assertIsNone(self.testee.tick)
        self.testee.execute.assert_not_called()

    def test_run_stop_not_set(self):
        self.testee.create = MagicMock()
        self.testee.terminate = MagicMock()
        self.testee.stop = MagicMock()
        self.testee.execute = MagicMock()
        self.testee.sleep = MagicMock()
        self.testee.stop.is_set.side_effect = [False, False, True]
        self.testee.run()
        self.testee.create.assert_called_once()
        self.testee.terminate.assert_called_once()
        self.testee.sleep.wait.assert_called_once()
        self.testee.sleep.clear.assert_called_once()
        self.testee.execute.assert_called_once()

    def test_run_breaks_on_execute(self):
        self.testee.create = MagicMock()
        self.testee.terminate = MagicMock()
        self.testee.stop = MagicMock()
        self.testee.sleep = MagicMock()
        self.testee.execute = MagicMock()
        self.testee.stop.is_set.return_value = False
        self.testee.execute.side_effect = [False, True]
        self.testee.run()
        self.testee.create.assert_called_once()
        self.testee.terminate.assert_called_once()
        self.testee.sleep.wait.assert_called_once()
        self.testee.sleep.clear.assert_called_once()
        self.assertEqual(self.testee.stop.is_set.call_count, 1)

