# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.concerns.base import Concern
from p2p0mq.constants import HEART_BEAT_INTERVAL, TRACE, SPEED_FAST, HEART_BEAT_SLOW_DOWN, HEART_BEAT_MAX_INTERVAL, \
    UNRESPONSIVE_THRESHOLD, ASK_AROUND_INTERVAL
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('p2p0mq.concern.ask')


class AskAroundConcern(Concern):
    """
    Routing helper that asks connected peers about ways to reach a
    certain peer.
    """

    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(AskAroundConcern, self).__init__(
            name="ask around", command_id=b'r', *args, **kwargs)

    def compose_ask_around_message(self, peer, exclude, breadcrumbs=None):
        """
        Creates a message asking about a peer's whereabouts.

        Arguments:
            peer (Peer):
                The peer we're interested in
            exclude:
                The list of peers we should not ask about it.
            breadcrumbs:
                The path the message took to reach us.
        """
        if peer.next_ask_around_time is not None:
            if peer.next_ask_around_time < self.app.tick:
                return []

        peer.next_ask_around_time = self.app.tick + ASK_AROUND_INTERVAL
        peer.last_ask_around_time = self.app.tick
        if breadcrumbs is None:
            breadcrumbs = [self.app.uuid]
        else:
            breadcrumbs = breadcrumbs + [self.app.uuid]

        return [(SPEED_FAST, Message(
            source=self.app.uuid,
            to=dest.uuid,
            previous_hop=None,
            next_hop=dest.uuid,
            command=self.command_id,
            reply=False,
            handler=self,
            target=peer.uuid,
            breadcrumbs=breadcrumbs
        )) for dest in self.app.peers_connected
            if (peer.uuid != dest.uuid) and (dest.uuid not in exclude)]

    def process_request(self, message):
        """ Handler on the receiver side for ask around requests. """
        try:
            target = message.payload['target']
            breadcrumbs = message.payload['breadcrumbs']
        except KeyError:
            logger.error("Malformed ask-around message", exc_info=True)
            return None

        logger.debug("We're being asked about peer %r from %r",
                     target, breadcrumbs)
        if target == self.app.uuid:
            logger.critical("We should not be here! Request to find a "
                            "peer and that peer is me. %r failed to notice "
                            "that.", message.previous_hop)
            return None

        found_peer = None
        with self.app.peers_lock:
            if target in self.app.peers:
                peer = self.app.peers[target]
                found_peer = peer
                if peer.state_connected:
                    logger.log(TRACE, "Found the peer and is connected.")
                    return SPEED_FAST, message.create_reply(
                        target=target,
                        breadcrumbs=breadcrumbs)
            else:
                logger.log(TRACE, "Haven't seen this peer.")
                peer = Peer(uuid=target)
                self.app.peers[target] = peer

        logger.log(TRACE, "Don't have this peer. Will ask around")
        return self.compose_ask_around_message(
            peer=target,
            exclude=[message.source, message.previous_hop, found_peer],
            breadcrumbs=breadcrumbs
        )

    def process_reply(self, message):
        """ Handler on the sender side for ask around reply. """

        try:
            target = message.payload['target']
            breadcrumbs = message.payload['breadcrumbs']
        except KeyError:
            logger.error("Malformed ask-around reply to message",
                         exc_info=True)
            return None

        logger.debug("Received hint from %r about peer %r; tell %r",
                     message.previous_hop, target, breadcrumbs)

        myself = breadcrumbs[-1]
        if myself != self.app.uuid:
            logger.critical("Invalid format of the breadcrumbs (%r)",
                            breadcrumbs)
        breadcrumbs = breadcrumbs[:-1]

        with self.app.peers_lock:
            if target in self.app.peers:
                logger.log(TRACE, "We have this peer in our store.")
                peer = self.app.peers[target]
            else:
                logger.debug("Received a replay for an unknown peer: %r.",
                             message)
                peer = Peer(uuid=target)
                self.app.peers[target] = peer
            peer.via = message.previous_hop

        if len(breadcrumbs) == 0:
            return None

        requester = breadcrumbs[-1]
        logger.log(TRACE, "Informing %r about this peer", requester)
        return SPEED_FAST, message.create_reply(
            source=self.app.uuid,
            to=requester,
            previous_hop=None,
            next_hop=requester,
            target=target,
            breadcrumbs=breadcrumbs)
