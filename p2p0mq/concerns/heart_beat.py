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

    def compose_heart_beat_request(self, peer):
        """
        Creates a request for a heartbeat.

        Arguments:
            peer (Peer):
                The peer we should send the message to.
        """
        return Message(
            source=self.app.uuid,
            to=peer.uuid,
            previous_hop=None,
            next_hop=peer.uuid if peer.state_connected else peer.via,
            command=self.command_id,
            reply=False,
            handler=self,
        )

    def execute(self):
        """
        Called from application thread on each thread loop.

        We go through each CONNECTED, ROUTED or UNREACHABLE peer and send a
        heart beat message if the timeout has been reached.
        """
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

    def expired_peer(self, peer, messages):
        """
        A peer that has passed it's heart-beat time.

        If the time since last message is large enough either the
        NO_CONNECTION or UNRESPONSIVE state is set.

        For peers in NO_CONNECTION state no heart-beat will
        be scheduled, as it is the responsibility of
        :class:`~p2p0mq.concerns.connector.ConnectorConcern` to bring it out
        of that state.

        UNRESPONSIVE peers are only sent heart beat messages until they
        exit this state.

        Arguments:
            peer (Peer):
                The peer whose heart-beat timeout has expired.
            messages (list):
                The list where we append any messages we decide need sending.
        """
        if peer.last_heart_beat_time + NO_CONNECTION_THRESHOLD < self.app.tick:
            peer.state_no_connection = True
            return

        if peer.last_heart_beat_time + UNRESPONSIVE_THRESHOLD < self.app.tick:
            peer.state_unreachable = True

        peer.schedule_heart_beat(self.app)
        messages.append(self.compose_heart_beat_request(peer))

    def process_request(self, message):
        """
        A heart-beat request message has arrived.

        This is the handler on the receiver side for heart beat requests.
        We reset the heart beat timer for the peer that sent the message
        and we send a reply back.

        Arguments:
            message (Message):
                The message we have received.
        """
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                logger.error("Heart beat request from a peer we've never "
                             "seen before: %r", message)
                return

            peer.become_connected(message, self.app)
        return SPEED_FAST, message.create_reply()

    def process_reply(self, message):
        """
        A reply to our heart beat request has been received.

        This is the handler on the sender side for heart beat reply.
        We reset the heart beat timer for the peer.

        Arguments:
            message (Message):
                The message we have received.
        """
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                logger.error("Heart beat response from a peer we've never "
                             "seen before: %r", message)
                return

            peer.become_connected(message, self.app)
