# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.concerns.base import Concern
from p2p0mq.constants import HEART_BEAT_INTERVAL, TRACE, SPEED_FAST, HEART_BEAT_SLOW_DOWN, HEART_BEAT_MAX_INTERVAL, \
    UNRESPONSIVE_THRESHOLD
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('p2p0mq.concern.con')


class ConnectorConcern(Concern):
    """
    Connects peers.

    We continuously check for peers in initial state or peers
    that were requested to connect.

    To peers in initial state we instruct the sender to connect to them and,
    once connected, we send them the greeting message.
    """

    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(ConnectorConcern, self).__init__(
            name="connector", command_id=b'hello', *args, **kwargs)

    def process_request(self, message):
        """ Handler on the receiver side for connect requests. """
        logger.debug("Request to connect received: %s", message)
        with self.app.peers_lock:
            if message.source in self.app.peers:
                logger.debug("I already know peer %r", message.source)
                peer = self.app.peers[message.source]

                logger.log(TRACE, "previous host: %r, new host: %r",
                           peer.host, message.payload['host'])
                peer.host = message.payload['host']

                logger.log(TRACE, "previous port: %r, new port: %r",
                           peer.port, message.payload['port'])
                peer.port = message.payload['port']
            else:
                peer = Peer(
                    uuid=message.source,
                    host=message.payload['host'],
                    port=message.payload['port']
                )
                self.app.peers[message.source] = peer
                logger.debug("Never heard of such peer %r", message.source)
                logger.log(TRACE, "host: %r, port: %r",
                           peer.host, peer.port)

        peer.last_heart_beat_time = self.app.tick
        if peer.needs_reconnect:
            logger.debug("A connect will be attempted to %s as a result "
                         "of this request", peer)
            self.connect_peer(peer)
        else:
            peer.become_connected(
                message.source == message.previous_hop,
                message.previous_hop,
                self.app)

        return SPEED_FAST, message.create_reply(
            host=self.app.receiver.bind_address,
            port = self.app.receiver.bind_port,
        )

    def process_reply(self, message):
        """ Handler on the sender side for heart beat reply. """
        logger.debug("Reply for connect received: %s", message)
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                logger.error("Connect response to a peer we've never "
                             "seen before: %r", message)
                return

        peer.become_connected(
            message.source == message.previous_hop,
            message.previous_hop,
            self.app)

    def connect_peer(self, peer, first=True):
        """ Take steps to connect a peer. """
        if peer.next_heart_beat_time is not None and first:
            return
        message = Message(
            source=self.app.uuid,
            to=peer.uuid,
            previous_hop=None,
            next_hop=peer.uuid,
            command=self.command_id,
            reply=False,
            handler=self,
            host=self.app.receiver.bind_address,
            port=self.app.receiver.bind_port,
        )
        if first:
            peer.next_heart_beat_time = self.app.tick + UNRESPONSIVE_THRESHOLD
            peer.slow_heart_beat_down = 0
        else:
            peer.schedule_heart_beat(self.app)

        self.app.sender.connection_queue.enqueue({peer: message})

    def reconnect_peer(self, peer):
        """ Take steps to connect a peer. """
        if peer.next_heart_beat_time < self.app.tick:
            self.connect_peer(peer, first=False)

    def connecting_peer(self, peer):
        """
        Take steps to check a peer we attempted to connect.
        """
        if peer.next_heart_beat_time < self.app.tick:
            peer.state_no_connection = True
            peer.last_heart_beat_time = self.app.tick

    def execute(self):
        """ Called from application thread on each thread loop. """
        if self.app.uuid == b'2~2~2~2':
            return
        with self.app.peers_lock:
            for peer in self.app.peers.values():

                # Skip peers that have no chance at connecting.
                if peer.host is None:
                    continue

                if peer.state_connecting:
                    self.connecting_peer(peer)
                elif peer.state_initial:
                    self.connect_peer(peer)
                elif peer.state_no_connection:
                    self.reconnect_peer(peer)

    def send_failed(self, message, exc=None):
        """
        We are informed that one of our messages failed to send.

        This call is made in the context of the sending thread.

        Return the message to be re-queued (can be same message).
        """
        with self.app.peers_lock:
            peer = self.app.peers[message.to]
            peer.state_no_connection = True
        return None

    def message_sent(self, message):
        """
        We are informed that one of our messages was sent.

        This call is made in the context of the sending thread.
        """
        with self.app.peers_lock:
            peer = self.app.peers[message.to]
            peer.state_connecting = True

    def message_dropped(self, message):
        """
        We are informed that one of our messages was dropped.

        This call is made in the context of the sending thread.
        """
        with self.app.peers_lock:
            peer = self.app.peers[message.to]
            peer.state_no_connection = True
