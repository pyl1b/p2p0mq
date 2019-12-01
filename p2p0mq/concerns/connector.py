# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.concerns.base import Concern
from p2p0mq.constants import HEART_BEAT_INTERVAL, TRACE, SPEED_FAST, HEART_BEAT_SLOW_DOWN, HEART_BEAT_MAX_INTERVAL, \
    UNRESPONSIVE_THRESHOLD, UNRESPONSIVE_RECONNECT_WAIT
from p2p0mq.message import Message
from p2p0mq.peer import Peer

logger = logging.getLogger('p2p0mq.concern.con')


class ConnectorConcern(Concern):
    """
    Connects peers.

    We continuously check for peers in initial state or peers
    that were requested to connect.

    For peers in initial state we instruct the sender to connect to them and,
    once connected, we send them the greetings message.
    """

    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(ConnectorConcern, self).__init__(
            name="connector", command_id=b'hello', *args, **kwargs)

    def connect_peer(self, peer, first=True):
        """
        Take steps to connect a peer.

        Arguments:
            peer (Peer):
                The peer to connect to.
            first (bool):
                tells if this is a connect or reconnect attempt.
        """

        # Filter peers that we have send a connect request in previous loops
        # but we haven't seen a reply, yet.
        if peer.next_heart_beat_time is not None and first:
            return

        # Compose the message.
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
            # Compute the timeout.
            peer.next_heart_beat_time = self.app.tick + UNRESPONSIVE_THRESHOLD
            peer.slow_heart_beat_down = 0
        else:
            # Take into consideration the history of the peer.
            peer.schedule_heart_beat(self.app)

        # We directly enqueue the message.
        self.app.sender.connection_queue.enqueue({peer: message})

    def reconnect_peer(self, peer):
        """
        Re-attempt to connect a peer that failed before.

        Arguments:
            peer(Peer):
                The peer in question.
        """
        if peer.next_heart_beat_time < self.app.tick:
            self.connect_peer(peer, first=False)

    def connecting_peer(self, peer):
        """
        Check a peer we attempted to connect.

        If the timeout set when the message was created has been exceeded
        the peer is :meth:`marked <~declare_no_connection>` as not connected.
        A new connection attempt is also set into the future through
        :meth:`~reconnect_peer`.

        Arguments:
            peer(Peer):
                The peer in question.
        """
        if peer.next_heart_beat_time < self.app.tick:
            self.declare_no_connection(peer)

    def declare_no_connection(self, peer):
        """
        We mark a peer as impossible to connect to.

        A reconnect attempt will also be scheduled after some seconds i
        nto the future. At that point :meth:`~reconnect_peer` will be invoked.

        Arguments:
            peer(Peer):
                The peer in question.
        """
        peer.state_no_connection = True
        peer.last_heart_beat_time = self.app.tick
        peer.next_heart_beat_time = \
            peer.last_heart_beat_time + UNRESPONSIVE_RECONNECT_WAIT

    def execute(self):
        """
        Called from application thread on each thread loop.

        The method will look into all peers and decide actions based on
        their state:

        * for new peers in INITIAL state a message is enqueued and \
        state is changed to CONNECTING;
        * for CONNECTING peers, if the timeout is exceeded the state is \
        changed to NO_CONNECTION;
        * for peers in NO_CONNECTION state a reconnect is attempted \
        if the time is right.

        Any peer that doesn't have a `host` set is ignored.
        """
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

    def process_request(self, message):
        """
        A peer requests to connect to local peer.

        This is the handler on the receiver side for connect requests.
        If we know this peer we update its details like host and port.
        If we don't, we create a new :class:`~p2p0mq.peer.Peer` and add it
        to the application.

        A new connection is attempted right away with new details if the
        state is INITIAL, NO_CONNECTION, or UNREACHABLE. For other states
        (CONNECTING, CONNECTED, ROUTED) we change state to either
        CONNECTED or ROUTED depending on the path the message arrived.

        A reply is composed with our details (host, port) and is send to the
        path it came from.

        Arguments:
            message (Message):
                The message to process.
        """
        logger.debug("Request to connect received: %s", message)
        if message.source in self.app.peers:
            logger.debug("I already know peer %r", message.source)
            with self.app.peers_lock:
                peer = self.app.peers[message.source]

            logger.log(TRACE, "previous host: %r, new host: %r, "
                              "previous port: %r, new port: %r",
                       peer.host, message.payload['host'],
                       peer.port, message.payload['port'])
            peer.host = message.payload['host']
            peer.port = message.payload['port']
        else:
            peer = Peer(
                uuid=message.source,
                host=message.payload['host'],
                port=message.payload['port']
            )
            self.app.add_peer(peer)
            logger.debug("Never heard of such peer %r", message.source)
            logger.log(TRACE, "host: %r, port: %r",
                       peer.host, peer.port)

        peer.last_heart_beat_time = self.app.tick
        if peer.needs_reconnect:
            logger.debug("A connect will be attempted to %s as a result "
                         "of this request", peer)
            self.connect_peer(peer)
        else:
            peer.become_connected(message, self.app)

        return SPEED_FAST, message.create_reply(
            host=self.app.receiver.bind_address,
            port=self.app.receiver.bind_port,
        )

    def process_reply(self, message):
        """
        A request to connect has been accepted by the peer.

        This is the handler on the sender side for connect requests.
        We update the details based on the information in the reply and
        change state of the peer to either CONNECTED or ROUTED
        depending on the path the message has arrived on.

        Arguments:
            message (Message):
                The message to process.
        """
        logger.debug("Reply for connect received: %s", message)
        with self.app.peers_lock:
            try:
                peer = self.app.peers[message.source]
            except KeyError:
                logger.error("Connect response to a peer we've never "
                             "seen before: %r", message)
                return

        logger.log(TRACE, "previous host: %r, new host: %r, "
                          "previous port: %r, new port: %r",
                   peer.host, message.payload['host'],
                   peer.port, message.payload['port'])
        peer.host = message.payload['host']
        peer.port = message.payload['port']
        peer.become_connected(message, self.app)

    def send_failed(self, message, exc=None):
        """
        We are informed that one of our messages failed to send.

        This call is made in the context of the sending thread.

        .. warning::
           As the handling of this message is special (
           see :meth:`p2p0mq.app.client.Sender.connect_peers`) this
           method is prohibited from re-issuing a message by returning it.
        """
        with self.app.peers_lock:
            self.declare_no_connection(self.app.peers[message.to])
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
            self.declare_no_connection(self.app.peers[message.to])
