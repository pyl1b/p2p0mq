# -*- coding: utf-8 -*-
"""
Unit tests for Sender.
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
import shutil
import tempfile
import uuid
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock, call

import zmq

from p2p0mq.app.client import Sender
from p2p0mq.app.local_peer import LocalPeer
from p2p0mq.concerns.base import Concern
from p2p0mq.constants import LOOP_CONTINUE, SPEED_MEDIUM, SPEED_FAST, SPEED_SLOW
from p2p0mq.errors import ValidationError
from p2p0mq.message import Message
from p2p0mq.message_queue.fast import FastMessageQueue
from p2p0mq.message_queue.slow import SlowMessageQueue
from p2p0mq.peer import Peer

logger = logging.getLogger('tests.p2p0mq.client')


class TestTestee(TestCase):
    def setUp(self):
        self.app = MagicMock(spec=LocalPeer)
        self.app._uuid = uuid.uuid4().hex.encode()
        self.app.uuid = self.app._uuid
        self.app.no_encryption = False
        self.testee = Sender(
            app=self.app,
            bind_address=None
        )

    def tearDown(self):
        if self.testee.socket is not None:
            self.testee.socket.close()
        self.testee.context.destroy()
        self.testee = None

    def test_init(self):
        self.testee = Sender(
            app=None,
            bind_address='1'
        )
        self.assertEqual(self.testee.bind_address, '1')
        self.assertEqual(self.testee.name, 'p2p0mq.C.th')
        self.assertIsNone(self.testee.bind_port)
        self.assertIsNone(self.testee.app)
        self.assertIsNotNone(self.testee.context)
        self.assertIsNone(self.testee.socket)
        self.assertFalse(self.testee.no_encryption)

    def test_create(self):
        self.testee.app.no_encryption = True
        self.testee.bind_address = "tcp://127.0.0.1:19999"
        self.testee.create()
        self.assertIsInstance(self.testee.socket, zmq.Socket)
        self.assertEqual(self.testee.socket.getsockopt(zmq.LINGER), 0)

    def test_send_message(self):
        message = MagicMock(spec=Message)
        message.valid_for_send.return_value = False
        with self.assertRaises(AssertionError):
            self.testee.send_message(message)

        message = MagicMock(spec=Message)
        message.valid_for_send.return_value = True
        message.handler = MagicMock(spec=Concern)
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.assertIsNone(self.testee.send_message(message))
        message.valid_for_send.assert_called_once()
        self.testee.socket.send_multipart.assert_called_once()
        message.handler.message_sent.assert_called_once()

        message = MagicMock(spec=Message)
        message.valid_for_send.return_value = True
        message.handler = MagicMock(spec=Concern)
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.socket.send_multipart.side_effect = zmq.error.ZMQError()
        message.time_to_live = 1
        self.app.tick = 2
        message.handler.send_failed.return_value = 'x'
        result = self.testee.send_message(message)
        self.assertIsNone(result)
        message.handler.message_dropped.assert_called_once_with(message)
        message.handler.send_failed.assert_not_called()

        message = MagicMock(spec=Message)
        message.valid_for_send.return_value = True
        message.handler = MagicMock(spec=Concern)
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.socket.send_multipart.side_effect = zmq.error.ZMQError()
        message.time_to_live = 3
        self.app.tick = 2
        message.handler.send_failed.return_value = 'x'
        result = self.testee.send_message(message)
        self.assertEqual(result, 'x')
        message.handler.message_dropped.assert_not_called()

    def test_execute_queue(self):
        message1 = MagicMock(spec=Message)
        message2 = MagicMock(spec=Message)
        queue = MagicMock(spec=SlowMessageQueue)
        queue.dequeue.return_value = [message1, message2]
        self.testee.send_message = MagicMock()
        self.testee.send_message.return_value = None
        self.testee.execute_queue(queue)
        self.assertEqual(self.testee.send_message.call_count, 2)

        message1 = MagicMock(spec=Message)
        message2 = MagicMock(spec=Message)
        queue = MagicMock(spec=SlowMessageQueue)
        queue.dequeue.return_value = [message1, message2]
        ret_message = MagicMock(spec=Message)
        self.testee.send_message.return_value = ret_message
        self.testee.execute_queue(queue)
        queue.enqueue.assert_called_with(ret_message)
        self.assertEqual(queue.enqueue.call_count, 2)

    def test_connect_peers(self):
        queue = SlowMessageQueue()
        queue.empty = MagicMock()
        queue.empty.return_value = True
        self.testee.connection_queue = queue
        self.testee.fast_queue = MagicMock()
        self.testee.connect_peers()
        self.testee.fast_queue.assert_not_called()
        queue.empty.assert_called_once()

        queue = SlowMessageQueue()
        queue.empty = MagicMock()
        queue.empty.return_value = False
        queue.dequeue = MagicMock()
        queue.dequeue.return_value = [1, 2]
        self.testee.connection_queue = queue
        with self.assertRaises(AssertionError):
            self.testee.connect_peers()

        queue = SlowMessageQueue()
        queue.empty = MagicMock()
        queue.empty.return_value = False
        queue.dequeue = MagicMock()
        queue.dequeue.return_value = [[1, 1], [1, 2]]
        self.testee.connection_queue = queue
        with self.assertRaises(AssertionError):
            self.testee.connect_peers()

        message1 = MagicMock(spec=Message)
        message2 = MagicMock(spec=Message)
        peer1 = MagicMock(spec=Peer)
        peer1.uuid = '11111'
        peer1.address = 'a1'
        peer2 = MagicMock(spec=Peer)
        peer2.uuid = '22222'
        peer2.address = 'a1'
        queue = SlowMessageQueue()
        queue.enqueue({peer1: message1})
        queue.enqueue({peer2: message2})
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.connection_queue = queue
        self.assertFalse(hasattr(peer1,'_first_connect'))
        self.assertFalse(hasattr(peer2,'_first_connect'))
        self.testee.fast_queue = MagicMock(spec=FastMessageQueue)
        self.testee.connect_peers()
        self.assertTrue(hasattr(peer1,'_first_connect'))
        self.assertTrue(hasattr(peer2,'_first_connect'))
        self.assertEqual(self.testee.socket.connect.call_count, 2)
        self.testee.socket.connect.assert_called_with('a1')
        self.assertEqual(self.testee.fast_queue.enqueue.call_count, 2)
        self.testee.fast_queue.enqueue.assert_called_with(message2)

        self.testee.connection_queue = SlowMessageQueue()
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.connect_peers()
        self.testee.socket.assert_not_called()

        self.testee.socket = MagicMock(spec=zmq.Socket)
        peer1 = MagicMock(spec=Peer)
        peer1.uuid = '11111'
        peer1.address = 'a1'
        setattr(peer1, '_first_connect', peer1.uuid)
        self.testee.connection_queue = MagicMock(spec=SlowMessageQueue)
        self.testee.connection_queue.empty.side_effect = [False, True]
        self.testee.connection_queue.dequeue.return_value = [{peer1: message1}]
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.connect_peers()
        self.testee.socket.setsockopt.assert_not_called()
        self.testee.socket.connect.assert_called_once_with('a1')
        self.testee.fast_queue.enqueue.assert_called_with(message1)

        message1.handler = MagicMock()
        self.testee.socket = MagicMock(spec=zmq.Socket)
        peer1 = MagicMock(spec=Peer)
        peer1.uuid = '11111'
        peer1.address = 'a1'
        setattr(peer1, '_first_connect', peer1.uuid)
        self.testee.connection_queue = MagicMock(spec=SlowMessageQueue)
        self.testee.connection_queue.empty.side_effect = [False, True]
        self.testee.connection_queue.dequeue.return_value = [{peer1: message1}]
        self.testee.socket = MagicMock(spec=zmq.Socket)
        self.testee.socket.connect.side_effect = zmq.error.ZMQError()
        self.testee.connect_peers()
        message1.handler.send_failed.assert_called_once()

    def test_execute(self):
        self.testee.connect_peers = MagicMock()
        self.testee.execute_queue = MagicMock()
        self.assertEqual(self.testee.execute(), LOOP_CONTINUE)
        self.testee.execute_queue.assert_has_calls(
            [call(self.testee.fast_queue),
             call(self.testee.medium_queue),
             call(self.testee.slow_queue)])
        self.testee.connect_peers.assert_called_once()

    def test_enqueue(self):
        Message.validate_messages_for_send = MagicMock()
        Message.validate_messages_for_send.return_value = False
        with self.assertRaises(AssertionError):
            self.testee.enqueue(1, 2)

        Message.validate_messages_for_send.return_value = True
        with self.assertRaises(ValidationError):
            self.testee.enqueue(1, 202020)

        message = MagicMock(spec=Message)

        self.testee.sleep = MagicMock()
        self.testee.fast_queue.enqueue = MagicMock()
        self.testee.medium_queue.enqueue = MagicMock()
        self.testee.slow_queue.enqueue = MagicMock()
        self.testee.enqueue(message, SPEED_MEDIUM)
        self.testee.fast_queue.enqueue.assert_not_called()
        self.testee.medium_queue.enqueue.assert_called_once()
        self.testee.slow_queue.enqueue.assert_not_called()
        self.testee.sleep.set.assert_called_once()

        self.testee.sleep = MagicMock()
        self.testee.fast_queue.enqueue = MagicMock()
        self.testee.medium_queue.enqueue = MagicMock()
        self.testee.slow_queue.enqueue = MagicMock()
        self.testee.enqueue(message, SPEED_FAST)
        self.testee.fast_queue.enqueue.assert_called_once()
        self.testee.medium_queue.enqueue.assert_not_called()
        self.testee.slow_queue.enqueue.assert_not_called()
        self.testee.sleep.set.assert_called_once()

        self.testee.sleep = MagicMock()
        self.testee.fast_queue.enqueue = MagicMock()
        self.testee.medium_queue.enqueue = MagicMock()
        self.testee.slow_queue.enqueue = MagicMock()
        self.testee.enqueue(message, SPEED_SLOW)
        self.testee.fast_queue.enqueue.assert_not_called()
        self.testee.medium_queue.enqueue.assert_not_called()
        self.testee.slow_queue.enqueue.assert_called_once()
        self.testee.sleep.set.assert_called_once()

    def test_enqueue_all(self):

        self.testee.sleep = MagicMock()
        self.testee.enqueue_all()
        self.testee.sleep.set.assert_not_called()

        self.testee.sleep = MagicMock()
        arg = {}
        self.testee.enqueue_all(requests=arg, replies=arg, routed=[])
        self.testee.sleep.set.assert_not_called()

        self.testee.sleep = MagicMock()
        arg = {
            SPEED_FAST: [],
            SPEED_MEDIUM: [],
            SPEED_SLOW: [],
        }
        self.testee.enqueue_all(requests=arg, replies=arg, routed=[])
        self.testee.sleep.set.assert_not_called()

        Message.validate_messages_for_send = MagicMock()
        Message.validate_messages_for_send.return_value = False
        self.testee.sleep = MagicMock()
        arg = {
            SPEED_FAST: [MagicMock(spec=Message)],
            SPEED_MEDIUM: [MagicMock(spec=Message)],
            SPEED_SLOW: [MagicMock(spec=Message)],
        }
        with self.assertRaises(AssertionError):
            self.testee.enqueue_all(requests=arg, replies=arg, routed=[])

        Message.validate_messages_for_send = MagicMock()
        Message.validate_messages_for_send.return_value = True
        self.testee.sleep = MagicMock()
        message = MagicMock(spec=Message)
        message.to = '11111'
        arg = {
            SPEED_FAST: [message],
            SPEED_MEDIUM: [message],
            SPEED_SLOW: [message],
        }
        self.testee.enqueue_all(requests=arg, replies=arg, routed=[])
        self.testee.sleep.set.assert_called_once()

    def enqueue_part(self, method, queue):
        message = MagicMock()
        Message.validate_messages_for_send = MagicMock()
        Message.validate_messages_for_send.return_value = False
        with self.assertRaises(AssertionError):
            method(self.testee, message)

        message = MagicMock()
        Message.validate_messages_for_send = MagicMock()
        Message.validate_messages_for_send.return_value = True
        queue.enqueue = MagicMock()
        self.testee.sleep = MagicMock()
        method(self.testee, message)
        queue.enqueue.assert_called_once_with(message)
        self.testee.sleep.set.assert_called_once()

    def test_enqueue_fast(self):
        self.enqueue_part(Sender.enqueue_fast, self.testee.fast_queue)

    def test_enqueue_slow(self):
        self.enqueue_part(Sender.enqueue_slow, self.testee.slow_queue)

    def test_enqueue_medium(self):
        self.enqueue_part(Sender.enqueue_medium, self.testee.medium_queue)
