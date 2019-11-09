# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.concerns.base import Concern
from p2p0mq.constants import (
    HEART_BEAT_INTERVAL, TRACE, SPEED_FAST, UNRESPONSIVE_THRESHOLD,
    NO_CONNECTION_THRESHOLD, HEART_BEAT_MAX_INTERVAL,
    HEART_BEAT_SLOW_DOWN
)
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('p2p0mq.concern.hb')


class HeartBeatConcern(Concern):
    """
    Manages the heart-beat signal between peers.
    """

    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(HeartBeatConcern, self).__init__(
            name="heart-beat", command_id=b'hb', *args, **kwargs)

    def expired_peer(self, peer, messages):
        """ A peer that has passed it's heart-beat time. """
        if peer.last_heart_beat_time + UNRESPONSIVE_THRESHOLD < self.app.tick:
            peer.state_unreachable = True
        elif peer.last_heart_beat_time + NO_CONNECTION_THRESHOLD < self.app.tick:
            peer.state_no_connection = True
            return

        peer.schedule_heart_beat(self.app)
        messages.append(self.compose_heart_beat_request(peer))

    def execute(self):
        """ Called from application thread on each thread loop. """
        messages = []
        with self.app.peers_lock:
            for peer in self.app.peers.values():
                if peer.does_heart_beat:
                    if peer.next_heart_beat_time <= self.app.tick:
                        self.expired_peer(peer, messages)

        logger.log(TRACE, "%d messages to enqueue: %r",
                   len(messages), messages)
        if len(messages):
            self.app.sender.enqueue_fast(messages)

    def compose_heart_beat_request(self, peer):
        """ Creates a request for a heartbeat. """
        return Message(
            source=self.app.uuid,
            to=peer.uuid,
            previous_hop=None,
            next_hop=peer.uuid if peer.state_connected else peer.via,
            command=self.command_id,
            reply=False,
            handler=self,
        )

    def process_request(self, message):
        """ Handler on the receiver side for heart beat requests. """
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                logger.error("Heart beat response to a peer we've never "
                             "seen before: %r", message)
                return

            peer.reset_heart_beat(self.app)
        return SPEED_FAST, message.create_reply()

    def process_reply(self, message):
        """ Handler on the sender side for heart beat reply. """
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                peer = Peer()
                self.app.peers[message.source] = peer

            peer.reset_heart_beat(self.app)
