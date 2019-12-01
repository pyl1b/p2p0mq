# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading
from unittest import TestCase
from unittest.mock import MagicMock

from p2p0mq.concerns.ask_around import AskAroundConcern
from p2p0mq.message import Message
from p2p0mq.message_queue.slow import SlowMessageQueue
from p2p0mq.router import Router

logger = logging.getLogger('tests.p2p0mq.router')


class TestKoNetThreadNoDb(TestCase):
    def setUp(self):
        self.testee = Router()

    def tearDown(self):
        self.testee = None

    def test_init(self):
        self.testee = Router()
        self.assertIsNone(self.testee.default_route)

    def test_process_routes(self):

        queue = SlowMessageQueue()
        message1 = MagicMock(spec=Message)
        message1.to = '123'
        self.testee.uuid = '123'
        queue.enqueue(message1)
        with self.assertRaises(AssertionError):
            self.testee.process_routes(queue)

        queue = SlowMessageQueue()
        message1 = MagicMock(spec=Message)
        message1.to = None
        queue.enqueue(message1)
        with self.assertRaises(AssertionError):
            self.testee.process_routes(queue)

        queue = SlowMessageQueue()
        message1 = MagicMock(spec=Message)
        message1.to = 1111
        message1.time_to_live = 10
        message1.previous_hop = 888
        message1.source = 888
        queue.enqueue(message1)
        message2 = MagicMock(spec=Message)
        message2.to = 2222
        message2.time_to_live = 10
        message2.previous_hop = 888
        message2.source = 888
        queue.enqueue(message2)
        self.testee.peers = {}
        self.testee.peers_lock = threading.Lock()
        self.testee.tick = 9
        cm = MagicMock(spec=AskAroundConcern)
        cm.compose_ask_around_message.return_value = [MagicMock(spec=Message)]
        self.testee.concerns = {
            "ask around": cm
        }
        self.testee.drop_routed_message = MagicMock()
        result = self.testee.process_routes(queue)
        self.assertEqual(len(result), 2)
        self.assertEqual(self.testee.drop_routed_message.call_count, 2)
