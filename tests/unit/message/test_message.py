# -*- coding: utf-8 -*-
"""
Unit tests for Mesage.
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
import shutil
import tempfile
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock

from umsgpack import packb

from p2p0mq.app.local_peer import LocalPeer
from p2p0mq.concerns.base import Concern
from p2p0mq.constants import MESSAGE_TYPE_REQUEST, MESSAGE_TYPE_REPLY
from p2p0mq.message import Message

logger = logging.getLogger('tests.p2p0mq.message')


class TestTestee(TestCase):
    def setUp(self):
        self.app = MagicMock(spec=LocalPeer)
        self.app.tick = 111
        self.handler = MagicMock(spec=Concern)
        self.testee = Message(
            source='src',
            to='to',
            previous_hop='previous_hop',
            next_hop='next_hop',
            command='command',
            reply=False,
            handler=self.handler,
            time_to_live=1.2,
            alpha=1,
            beta=2,
            gamma=3,
            delta=4
        )

    def tearDown(self):
        self.testee = None

    def test_init(self):
        self.testee = Message()
        self.assertIsNone(self.testee.source)
        self.assertIsNone(self.testee.to)
        self.assertIsNone(self.testee.previous_hop)
        self.assertIsNone(self.testee.next_hop)
        self.assertIsNone(self.testee.handler)
        self.assertIsNone(self.testee.command)
        self.assertEqual(self.testee.kind, MESSAGE_TYPE_REQUEST)
        self.assertIsInstance(self.testee.payload, dict)
        self.assertEqual(len(self.testee.payload), 0)
        self.assertGreater(self.testee.time_to_live, 1572974887)

        self.testee = Message(
            source='1', to='2',
            previous_hop='previous_hop',
            next_hop='next_hop',
            command='3',
            reply=True,
            handler=self,
            time_to_live=5,
        )
        self.assertEqual(self.testee.source, '1')
        self.assertEqual(self.testee.to, '2')
        self.assertEqual(self.testee.previous_hop, 'previous_hop')
        self.assertEqual(self.testee.next_hop, 'next_hop')
        self.assertEqual(self.testee.command, '3')
        self.assertEqual(self.testee.kind, MESSAGE_TYPE_REPLY)
        self.assertEqual(self.testee.handler, self)
        self.assertGreater(self.testee.time_to_live, 1572974918)
        self.assertEqual(len(self.testee.payload), 0)

        self.testee = Message(alpha=1, beta=2, gamma=3, delta=4)
        self.assertDictEqual(
            self.testee.payload, {
                'alpha': 1,
                'beta': 2,
                'gamma': 3,
                'delta': 4
            }
        )

    def test_encode(self):
        bbb = self.testee.encode('uuid')
        self.assertIsInstance(bbb, tuple)
        self.assertEqual(len(bbb), 6)
        self.assertEqual(bbb[0], 'next_hop')
        self.assertEqual(bbb[1], 'src')
        self.assertEqual(bbb[2], b'')
        self.assertEqual(bbb[3], b'\01')
        self.assertEqual(bbb[4], 'command')
        self.assertIn(b'alpha', bbb[5])
        self.assertIn(b'beta', bbb[5])
        self.assertIn(b'gamma', bbb[5])
        self.assertIn(b'delta', bbb[5])

    def test_valid_for_send(self):

        msg = Message()
        self.assertFalse(msg.valid_for_send(app=self.app))
        msg = Message(
            to='to'
        )
        self.assertFalse(msg.valid_for_send(app=self.app))
        msg = Message(
            to='to',
            next_hop = 'next_hop',
        )
        self.assertFalse(msg.valid_for_send(app=self.app))
        msg = Message(
            source='src',
            to='to',
            next_hop = 'next_hop',
        )
        self.assertFalse(msg.valid_for_send(app=self.app))
        msg = Message(
            command='command',
            source='src',
            to='to',
            next_hop = 'next_hop',
        )
        self.assertFalse(msg.valid_for_send(app=self.app))
        msg = Message(
            handler=self.handler,
            command='command',
            source='src',
            to='to',
            next_hop = 'next_hop',
        )
        self.assertTrue(msg.valid_for_send(app=self.app))
        msg = Message(
            handler=self.handler,
            command='command',
            source='src',
            to='to',
            next_hop = 'next_hop',
            time_to_live=100
        )
        self.assertTrue(msg.valid_for_send(app=self.app))

