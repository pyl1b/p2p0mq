# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.constants import PROCESS_LIMIT_PER_LOOP, ASK_AROUND_INTERVAL, TRACE
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('p2p0mq')


class Router(object):
    """
    Manages the routing.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(Router, self).__init__(*args, **kwargs)
        self.default_route = None

    def process_routes(self, queue):
        """
        Processing the routing requests during the application loop.

        The messages that need routing are detected by the server (Receiver)
        and placed in a distinct queue. The application then calls
        this method to process the messages.
        """
        logger.log(TRACE, "Processing routes...")
        messages = []
        for i in range(min(PROCESS_LIMIT_PER_LOOP, len(queue))):
            # Get a message.
            message = queue.dequeue()
            logger.log(TRACE, "Routing message %r", message)
            if len(message) == 0:
                break
            assert len(message) == 1
            message = message[0]

            # The message should not be directed at this instance.
            assert message.to != self.uuid
            assert message.to is not None

            if message.to in self.peers:
                logger.log(TRACE, "Destination known")

                # We have heard of this peer before.
                peer = self.peers[message.to]

                if peer.state_connected:
                    logger.log(TRACE, "Destination known and connected")
                    # Attempt to send the message directly to the peer.
                    messages.append(message)
                    continue
                logger.log(TRACE, "Destination known but not connected")

                if peer.via is not None:
                    # The peer has some routing hints.
                    logger.log(TRACE,
                               "Peer routes all messages through %r",
                               peer.via)
                    message.next_hop = peer.via
                    messages.append(message)
                    continue
                logger.log(TRACE, "Peer doesn't have a proxy set")
            else:
                logger.log(TRACE, "Unknown peer; creating it")
                peer = Peer(uuid=message.to)
                with self.peers_lock:
                    self.peers[message.to] = peer

            if self.default_route is not None:
                # If we have a default route all messages not explicitly
                # routed using via will go there.
                logger.log(TRACE, "Using default route")
                message.next_hop = self.default_route
                messages.append(message)
                continue
            logger.log(TRACE, "Default route is not set")

            if message.time_to_live < self.tick:
                # Add it back to the queue and ask around.
                logger.log(
                    TRACE, "Message timeout %r not reached",
                    message.time_to_live)
                queue.enqueue(message)
                messages.extend(self.ask_around_for_peer(message.to))
                continue

            messages.extend(
                self.concerns["ask around"].compose_ask_around_message(
                    peer, [message.previous_hop, message.source]))

            # The last resort is to drop the message.
            logger.log(TRACE, "Dropping routed message")
            self.drop_routed_message(message)

        logger.log(TRACE, "Done processing routes.")
        return messages

    def drop_routed_message(self, message):
        pass
