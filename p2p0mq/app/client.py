# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

import zmq

from p2p0mq.constants import (
    SPEED_MEDIUM, SPEED_FAST, SPEED_SLOW,
    LOOP_CONTINUE, TRACE, ISOLATE, HEART_BEAT_INTERVAL, TRACE_PACKETS, TRACE_NET)
from p2p0mq.errors import ValidationError, MissingCertificateError
from p2p0mq.message import Message
from p2p0mq.message_queue.fast import FastMessageQueue
from p2p0mq.message_queue.medium import RegularMessageQueue
from p2p0mq.message_queue.slow import SlowMessageQueue
from p2p0mq.peer import Peer
from p2p0mq.utils.thread.netthread import KoNetThread

logger = logging.getLogger('p2p0mq.app.c')


class Sender(KoNetThread):
    """
    A thread that sends requests and information
    to peers.

    These are "clients" according to the
    [zmq_curve](http://api.zeromq.org/4-1:zmq-curve) which states that:
    > A socket using CURVE can be either client or server,
    > at any moment, but not both.
    > The role is independent of bind/connect direction.

    To become a CURVE client, the local peer sets the ZMQ_CURVE_SERVERKEY
    option with the long-term public key of the server it intends to
    connect to, or accept connections from, next. The local peer then
    sets the ZMQ_CURVE_PUBLICKEY and ZMQ_CURVE_SECRETKEY
    options with its client long-term key pair.

    The sender provides three queues:
    - fast (sends all available messages every loop);
    - medium (sends oldest message of each peer every loop);
    - slow (sends oldest overall message;one message per loop).
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(Sender, self).__init__(*args, **kwargs)
        self.name = 'p2p0mq.C.th' if self.app is None or self.app.uuid is None \
            else ('%s-p2p0mq.C.th' % self.app.uuid[-4:-1].decode("utf-8"))
        self.slow_queue = SlowMessageQueue()
        self.medium_queue = RegularMessageQueue()
        self.fast_queue = FastMessageQueue()
        self.connection_queue = SlowMessageQueue()

    def create(self):
        """ Called at thread start to initialize the state. """
        logger.debug("Sender is being created...")
        super().create()

        # The ROUTER socket type talks to a set of peers, using explicit
        # addressing so that each outgoing message is sent to a specific
        # peer connection. ROUTER works as an asynchronous replacement
        # for REP, and is often used as the basis for servers that talk
        # to DEALER clients.
        # https://rfc.zeromq.org/spec:28/REQREP/#the-router-socket-type
        new_socket = self.context.socket(zmq.ROUTER)

        # Do not keep messages in memory that were not send yet when
        # we attempt to close the socket.
        # http://api.zeromq.org/2-1:zmq-setsockopt#toc15
        new_socket.setsockopt(zmq.LINGER, 0)

        # The ZMQ_IDENTITY option shall set the identity of
        # the specified socket. Socket identity determines if
        # existing ØMQ infrastructure (message queues,
        # forwarding devices) shall be identified with a
        # specific application and persist across multiple
        # runs of the local peer.
        #
        # If the socket has no identity, each run of an
        # application is completely separate from other runs.
        # However, with identity set the socket shall re-use
        # any existing ØMQ infrastructure configured by the
        # previous run(s). Thus the local peer may receive
        # messages that were sent in the meantime, message
        # queue limits shall be shared with previous run(s)
        # and so on.

        # http://api.zeromq.org/2-1:zmq-setsockopt#toc6
        new_socket.setsockopt(zmq.IDENTITY, self.app.uuid)
        new_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)

        if not self.app.no_encryption:
            logger.debug(
                "Application %r loads certificates for its Sender from %s",
                self.app.uuid, self.app.private_file)
            client_public, client_secret = zmq.auth.load_certificate(
                self.app.private_file)

            # http://api.zeromq.org/4-1:zmq-curve
            # To become a CURVE client, the local peer sets the
            # ZMQ_CURVE_SERVERKEY option with the long-term public key
            # of the server it intends to connect to, or accept
            # connections from, next.
            # new_socket.curve_serverkey = \
            #     str(peer_data["serverkey"]).encode('ascii')

            # The local peer then sets the ZMQ_CURVE_PUBLICKEY and
            # ZMQ_CURVE_SECRETKEY options with its client long-term key pair.
            # Set long term secret key (for server)
            new_socket.curve_publickey = client_public
            new_socket.curve_secretkey = client_secret

        # Connect to the address where we listen for incoming data
        # and send our requests.
        # address = self.address
        # new_socket.bind(address)
        # logger.debug("Sender bound to %s", address)

        self.socket = new_socket
        logger.debug("Sender has been created")
        return new_socket

    def terminate(self):
        """ Called at thread end to free resources. """
        super().terminate()
        assert self.socket is None
        logger.debug("sender (client) thread was terminated on app %r",
                     self.app.uuid)

    def send_message(self, message):
        """
        Encode and send a single message.

        If the socket throws an error:
        - we check if the time to live has expired, in which case we drop
        the message and inform the handler.
        - we tell the handler (Concern class) that we could not send it (
        by default the handler will give us back the same message to
        put it back in the queue until the message expires).

        Returns:
            The message to be enqueued or None. This is not a
            `(PRIORITY, message)` format.
        """
        try:
            logger.log(TRACE_NET, "encoding message for wire: %r", message)
            assert message.valid_for_send(self.app), "%r is invalid" % message
            encoded = message.encode(self.app.uuid)
            logger.log(TRACE_PACKETS, ">>>>>>>>>>>>>>>  %r", encoded)
            self.socket.send_multipart(encoded)
            message.handler.message_sent(message)
            return None

        except (zmq.error.ZMQError, KeyError) as exc:

            logger.log(TRACE, "Sender failed to send message",
                       exc_info=True)

            if message.time_to_live < self.app.tick:
                # Inform the handler that the message is being dropped.
                message.handler.message_dropped(message)
                return None
            else:
                # Inform the handler that the message could not be send.
                return message.handler.send_failed(message, exc)

    def execute_queue(self, queue):
        """
        Called during the execution step to send messages from a queue.

        The method takes one or more messages from a queue, encodes
        the message and sends it to the socket.
        """
        logger.log(TRACE, "sending queue %s", queue)

        to_send = queue.dequeue()
        for message in to_send:
            re_message = self.send_message(message)
            if re_message:
                logger.log(TRACE, "Failed message is being re-queued")
                queue.enqueue(re_message)
            else:
                logger.log(TRACE, "Message is being dropped")

    def connect_peers(self):
        """
        Called during the execution step to connect to peers.
        """
        while not self.connection_queue.empty():
            result = self.connection_queue.dequeue()
            assert len(result) == 1
            result = result[0]
            assert len(result) == 1
            peer = list(result)[0]
            message = result[peer]
            logger.debug("Connecting peer %r...", peer.uuid)
            try:
                if not hasattr(peer, '_first_connect'):
                    if not self.app.no_encryption:
                        # https://grokbase.com/t/zeromq/zeromq-dev/151j3cp0x0/about-curve-and-router-sockets
                        server_key = self.app.cert_key_by_uuid(
                            uuid=peer.uuid, public=True)
                        logger.debug("First connect to peer %r with key %r",
                                     peer.uuid, server_key)
                        if server_key is None:
                            message.handler.send_failed(
                                message, MissingCertificateError())
                            return
                        self.socket.curve_serverkey = server_key

                    setattr(peer, '_first_connect', peer.uuid)
                    self.socket.setsockopt(
                        zmq.CONNECT_RID, peer.uuid)

                self.socket.connect(peer.address)
                self.fast_queue.enqueue(message)
            except zmq.error.ZMQError as exc:
                message.handler.send_failed(message, exc)

    def execute(self):
        """
        Called to execute the main part of the thread.

        We're simply looping through our queues and ask each one
        to give us some messages that we then try to send.
        """
        self.connect_peers()
        for queue in (self.fast_queue, self.medium_queue, self.slow_queue):
            self.execute_queue(queue)
        return LOOP_CONTINUE

    def enqueue(self, message, priority=SPEED_MEDIUM):
        """ Adds one or more messages to internal queue
        to be send later. """
        assert Message.validate_messages_for_send(message, self.app)
        if priority == SPEED_MEDIUM:
            self.medium_queue.enqueue(message)
        elif priority == SPEED_FAST:
            self.fast_queue.enqueue(message)
        elif priority == SPEED_SLOW:
            self.slow_queue.enqueue(message)
        else:
            raise ValidationError("Unknown priority: %r", priority)
        self.sleep.set()

    def enqueue_all(self, requests=None, replies=None, routed=None):
        """ Enqueues all kinds of messages. """
        fast = []
        medium = []
        slow = []

        if requests is not None:
            if SPEED_FAST in requests:
                fast = requests[SPEED_FAST]
            if SPEED_MEDIUM in requests:
                medium = requests[SPEED_MEDIUM]
            if SPEED_SLOW in requests:
                slow = requests[SPEED_SLOW]

        if replies is not None:
            if SPEED_FAST in replies:
                fast = fast + replies[SPEED_FAST]
            if SPEED_MEDIUM in replies:
                medium = medium + replies[SPEED_MEDIUM]
            if SPEED_SLOW in replies:
                slow = slow + replies[SPEED_SLOW]

        if routed is not None:
            fast = fast + routed

        total = len(fast) + len(medium) + len(slow)

        if len(fast) > 0:
            assert Message.validate_messages_for_send(fast, self.app)
            self.fast_queue.enqueue(fast)
        if len(medium) > 0:
            assert Message.validate_messages_for_send(medium, self.app)
            self.medium_queue.enqueue(medium)
        if len(slow) > 0:
            assert Message.validate_messages_for_send(slow, self.app)
            self.slow_queue.enqueue(slow)

        if total > 0:
            self.sleep.set()

    def enqueue_fast(self, message):
        """ Adds one or more messages to internal fast queue
        to be send later. """
        assert Message.validate_messages_for_send(message, self.app)
        self.fast_queue.enqueue(message)
        self.sleep.set()

    def enqueue_slow(self, message):
        """ Adds one or more messages to internal slow queue
        to be send later. """
        assert Message.validate_messages_for_send(message, self.app)
        self.slow_queue.enqueue(message)
        self.sleep.set()

    def enqueue_medium(self, message):
        """ Adds one or more messages to internal medium queue
        to be send later. """
        assert Message.validate_messages_for_send(message, self.app)
        self.medium_queue.enqueue(message)
        self.sleep.set()
