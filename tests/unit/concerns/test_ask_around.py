# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading
from unittest import TestCase
from unittest.mock import MagicMock

from p2p0mq.app.local_peer import LocalPeer
from p2p0mq.concerns.ask_around import AskAroundConcern
from p2p0mq.constants import ASK_AROUND_INTERVAL, MESSAGE_TYPE_REQUEST, SPEED_FAST
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('tests.p2p0mq.ask')


class TestAskAroundConcern(TestCase):
    def setUp(self):
        self.app = MagicMock(spec=LocalPeer)
        self.testee = AskAroundConcern(app=self.app)
        self.app.tick = 11
        self.app.uuid = 99
        self.app.peers_lock = threading.Lock()
        self.app.peers = {}

    def tearDown(self):
        self.testee = None

    def test_init(self):
        self.assertEqual(self.testee.name, 'ask around')
        self.assertEqual(self.testee.command_id, b'r')

    def test_compose_ask_around_message__not_too_often(self):
        peer = MagicMock(spec=Peer)
        peer.next_ask_around_time = 10
        result = self.testee.compose_ask_around_message(
            peer=peer, exclude=[], breadcrumbs=None)
        self.assertEqual(len(result), 0)

    def test_compose_ask_around_message__no_peers(self):
        self.app.peers_connected = []
        peer = MagicMock(spec=Peer)
        peer.next_ask_around_time = None
        result = self.testee.compose_ask_around_message(
            peer=peer, exclude=[], breadcrumbs=None)
        self.assertEqual(
            peer.next_ask_around_time, self.app.tick+ASK_AROUND_INTERVAL)
        self.assertEqual(peer.last_ask_around_time, self.app.tick)
        self.assertEqual(len(result), 0)

    def test_compose_ask_around_message__one_peer_me(self):
        peer = MagicMock(spec=Peer)
        peer.next_ask_around_time = None
        peer.uuid = '1'

        self.app.peers_connected = [peer]

        result = self.testee.compose_ask_around_message(
            peer=peer, exclude=[], breadcrumbs=None)
        self.assertEqual(
            peer.next_ask_around_time, self.app.tick+ASK_AROUND_INTERVAL)
        self.assertEqual(peer.last_ask_around_time, self.app.tick)
        self.assertEqual(len(result), 0)

    def test_compose_ask_around_message__two_peers(self):
        peer1 = MagicMock(spec=Peer)
        peer1.next_ask_around_time = None
        peer1.uuid = '1'
        peer2 = MagicMock(spec=Peer)
        peer2.next_ask_around_time = None
        peer2.uuid = '2'

        self.app.peers_connected = [peer1, peer2]

        result = self.testee.compose_ask_around_message(
            peer=peer1, exclude=[peer2.uuid], breadcrumbs=None)
        self.assertEqual(len(result), 0)

        result = self.testee.compose_ask_around_message(
            peer=peer1, exclude=[], breadcrumbs=None)
        self.assertEqual(len(result), 1)
        speed, message = result[0]
        self.assertEqual(message.source, self.app.uuid)
        self.assertEqual(message.to, peer2.uuid)
        self.assertIsNone(message.previous_hop)
        self.assertEqual(message.next_hop, peer2.uuid)
        self.assertEqual(message.command, b'r')
        self.assertEqual(message.kind, MESSAGE_TYPE_REQUEST)
        self.assertEqual(message.handler, self.testee)
        self.assertEqual(message.payload['target'], peer1.uuid)
        self.assertListEqual(
            message.payload['breadcrumbs'], [self.app.uuid])

    def test_compose_ask_around_message__has_bread_crumbs(self):
        peer1 = MagicMock(spec=Peer)
        peer1.next_ask_around_time = None
        peer1.uuid = '1'
        peer2 = MagicMock(spec=Peer)
        peer2.next_ask_around_time = None
        peer2.uuid = '2'

        self.app.peers_connected = [peer1, peer2]

        result = self.testee.compose_ask_around_message(
            peer=peer1, exclude=[peer2.uuid], breadcrumbs=None)
        self.assertEqual(len(result), 0)

        result = self.testee.compose_ask_around_message(
            peer=peer1, exclude=[], breadcrumbs=['1', '2'])
        self.assertEqual(len(result), 1)
        speed, message = result[0]
        self.assertEqual(message.source, self.app.uuid)
        self.assertEqual(message.to, peer2.uuid)
        self.assertIsNone(message.previous_hop)
        self.assertEqual(message.next_hop, peer2.uuid)
        self.assertEqual(message.command, b'r')
        self.assertEqual(message.kind, MESSAGE_TYPE_REQUEST)
        self.assertEqual(message.handler, self.testee)
        self.assertEqual(message.payload['target'], peer1.uuid)
        self.assertListEqual(
            message.payload['breadcrumbs'], ['1', '2', self.app.uuid])

    def test_process_request__no_payload(self):
        message = MagicMock(spec=Message)
        message.payload = {}
        result = self.testee.process_request(message)
        self.assertIsNone(result)

    def test_process_request__no_breadcrumbs(self):
        message = MagicMock(spec=Message)
        message.payload = {
            'target': 1
        }
        result = self.testee.process_request(message)
        self.assertIsNone(result)

    def test_process_request__no_target(self):
        message = MagicMock(spec=Message)
        message.payload = {
            'breadcrumbs': 1
        }
        result = self.testee.process_request(message)
        self.assertIsNone(result)

    def test_process_request__same_target(self):
        message = MagicMock(spec=Message)
        message.previous_hop = 50
        message.payload = {
            'target': 99,
            'breadcrumbs': 1
        }
        result = self.testee.process_request(message)
        self.assertIsNone(result)

    def test_process_request(self):
        message = MagicMock(spec=Message)
        message.previous_hop = 50
        message.source = 777
        message.payload = {
            'target': 999,
            'breadcrumbs': 1
        }
        self.testee.compose_ask_around_message = MagicMock()
        self.testee.compose_ask_around_message.return_value = 999999
        result = self.testee.process_request(message)
        self.assertEqual(result, 999999)

    def test_process_request__known_target(self):
        message = MagicMock(spec=Message)
        message.previous_hop = 50
        message.source = 777
        message.payload = {
            'target': 999,
            'breadcrumbs': 1
        }
        message.create_reply = MagicMock()
        message.create_reply.return_value = 88888

        peer1 = MagicMock(spec=Peer)
        peer1.uuid = 999
        peer1.state_initial = True
        self.app.peers[999] = peer1

        result = self.testee.process_request(message)
        self.assertEqual(result[0], SPEED_FAST)
        self.assertEqual(result[1], 88888)



    def test_process_reply__no_payload(self):
        message = MagicMock(spec=Message)
        message.payload = {}
        result = self.testee.process_reply(message)
        self.assertIsNone(result)

    def test_process_reply__no_breadcrumbs(self):
        message = MagicMock(spec=Message)
        message.payload = {
            'target': 1
        }
        result = self.testee.process_reply(message)
        self.assertIsNone(result)

    def test_process_reply__no_target(self):
        message = MagicMock(spec=Message)
        message.payload = {
            'breadcrumbs': 1
        }
        result = self.testee.process_reply(message)
        self.assertIsNone(result)

