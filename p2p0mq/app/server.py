# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

import zmq
from zmq.backend.cython.error import zmq_errno

from p2p0mq.constants import (
    TRACE, RECEIVE_LIMIT_PER_LOOP, MESSAGE_TYPE_REPLY, MESSAGE_TYPE_REQUEST,
    LOOP_CONTINUE, ISOLATE, MESSAGE_TYPE_ROUTE,
    TRACE_PACKETS, TRACE_NET)
from p2p0mq.errors import ValidationError, MessageValidationError
from p2p0mq.message import Message
from p2p0mq.message_queue.slow import SlowMessageQueue
from p2p0mq.utils.thread.netthread import KoNetThread

logger = logging.getLogger('p2p0mq.app.s')


class Receiver(KoNetThread):
    """
    A thread that receives requests and information from peers.

    This is a "server" according to the
    [zmq_curve](http://api.zeromq.org/4-1:zmq-curve) which states that:
    > A socket using CURVE can be either client or server,
    > at any moment, but not both.
    > The role is independent of bind/connect direction.

    To become a CURVE server, the application sets the ZMQ_CURVE_SERVER
    option on the socket, and then sets the ZMQ_CURVE_SECRETKEY option
    to provide the socket with its long-term secret key.
    The application does not provide the socket with its long-term public key,
    which is used only by clients.

    The receiver listens to messages and creates messages out of them.
    The messages are then placed in the queue where the application
    will pick them up.

    For now the receiver understands two kinds of messages: requests and
    replies. However,adding additional types is easy: add a queue to the
    typed_queues member and create code on application side to offload
    this queue.

    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(Receiver, self).__init__(*args, **kwargs)
        self.name = 'p2p0mq.S.th' if self.app is None or self.app.uuid is None \
            else ('%s-p2p0mq.S.th' % self.app.uuid[-4:].decode("utf-8"))

        self.typed_queues = {
            MESSAGE_TYPE_REPLY: SlowMessageQueue(),
            MESSAGE_TYPE_REQUEST: SlowMessageQueue(),
            MESSAGE_TYPE_ROUTE: SlowMessageQueue(),
        }

    def create(self):
        """ Called at thread start to initialize the state. """
        logger.debug("Receiver is being created...")
        super().create()

        # The REP socket type acts as as service for a set of client peers,
        # receiving requests and sending replies back to the requesting
        # peers. It is designed for simple remote-procedure call models.
        # https://rfc.zeromq.org/spec:28/REQREP/#the-rep-socket-type
        new_socket = self.context.socket(zmq.ROUTER)
        # new_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)

        # Do not keep messages in memory that were not send yet when
        # we attempt to close the socket.
        # http://api.zeromq.org/2-1:zmq-setsockopt#toc15
        new_socket.setsockopt(zmq.LINGER, 0)

        # The ZMQ_IDENTITY option shall set the identity of
        # the specified socket. Socket identity determines if
        # existing ØMQ infrastructure (message queues,
        # forwarding devices) shall be identified with a
        # specific application and persist across multiple
        # runs of the application.
        #
        # If the socket has no identity, each run of an
        # application is completely separate from other runs.
        # However, with identity set the socket shall re-use
        # any existing ØMQ infrastructure configured by the
        # previous run(s). Thus the application may receive
        # messages that were sent in the meantime, message
        # queue limits shall be shared with previous run(s)
        # and so on.

        # http://api.zeromq.org/2-1:zmq-setsockopt#toc6
        new_socket.setsockopt(zmq.IDENTITY, self.app.uuid)

        if not self.app.no_encryption:
            logger.debug(
                "Application %r loads certificates for its Receiver from %s",
                self.app.uuid, self.app.private_file)
            server_public, server_secret = zmq.auth.load_certificate(
                self.app.private_file)

            # To become a CURVE server, the application sets the ZMQ_CURVE_SERVER
            # option on the socket,
            new_socket.curve_server = True

            # and then sets the ZMQ_CURVE_SECRETKEY option
            # to provide the socket with its long-term secret key.
            new_socket.curve_secretkey = server_secret

            # The application does not provide the socket with
            # its long-term public key, which is used only by clients.
            # socket.curve_publickey = server_public

        address = self.address.replace('0.0.0.0', '*')
        new_socket.bind(address)
        logger.debug("Receiver bound to %s", address)

        self.socket = new_socket
        logger.debug("Receiver was created")
        return new_socket

    def terminate(self):
        """ Called at thread end to free resources. """
        super().terminate()
        assert self.socket is None
        logger.debug("receiver (server) thread was terminated on app %r",
                     (self.app.uuid if self.app is not None else ''))

    def enqueue(self, message, messages):
        """
        Places a message in the appropriate queue.

        Messages not destined to us are placed in a special queue.
        Others are split based on their type field.

        Messages that have unknown types are silently discarded.
        """

        # Is this message destined to this instance?
        if message.to != self.app.uuid:
            queue = messages[MESSAGE_TYPE_ROUTE]
        else:
            try:
                queue = messages[message.kind]
            except KeyError:
                logger.error("Received unknown message type: %r",
                             message.kind)
                return

        # Update the queue.
        queue.append(message)
        logger.log(TRACE, "added to queue %r", queue)

    def update_queues(self, messages):
        """
        Places messages in queues all at once.

        The messages is a dictionary, with keys being the
        """
        for kind in self.typed_queues:
            from_queue = messages[kind]
            if len(from_queue) > 0:
                self.typed_queues[kind].enqueue(from_queue)

    def receive_message(self):
        """ Receive a single message. """
        message = None

        # noinspection PyBroadException
        try:
            # TODO: as we're converting the data right away do we need copy?
            raw_data = self.socket.recv_multipart(copy=True)
            logger.log(TRACE_PACKETS, "<<<<<<<<<<<<<<<   received message %r",
                       raw_data)
            message = Message.parse(raw_data, self.app.uuid)
            logger.log(TRACE_NET, "converted into message %r", message)
        except Exception:
            logger.error("Received invalid message",
                         exc_info=True)
        return message

    def execute(self):
        """
        Called to execute the main part of the thread.

        In implementations where this function is executed in a loop
        it is expected to return False to break the loop end terminate
        the thread.
        """

        # We store the messages here and we update the queues all at once.
        messages = dict([(kind, []) for kind in self.typed_queues])

        # Process incoming data.
        for i in range(RECEIVE_LIMIT_PER_LOOP):

            # Wait a bit for messages. Early exit if not.
            try:
                result = self.socket.poll(timeout=100, flags=zmq.POLLIN)
            except zmq.error.ZMQError:
                logger.error("Got ZMQ error %r in server poll",
                             zmq_errno(), exc_info=True)
                break
            if not (result & zmq.POLLIN):
                break

            # Process it.
            message = self.receive_message()
            if message is not None:
                self.enqueue(message, messages)

        # Place them in queue all at once.
        self.update_queues(messages)

        return LOOP_CONTINUE
