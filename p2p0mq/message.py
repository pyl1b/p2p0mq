# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
from time import time

from umsgpack import packb, unpackb

from p2p0mq.constants import MESSAGE_TYPE_REPLY, MESSAGE_TYPE_REQUEST, DEFAULT_TIME_TO_LIVE
from p2p0mq.errors import MessageValidationError

logger = logging.getLogger('p2p0mq.message')


class Message(object):
    """
    A message we send down the wire.
    """
    def __init__(self,
                 source=None, to=None,
                 previous_hop=None,
                 next_hop=None,
                 command=None,
                 reply=False,
                 handler=None,
                 time_to_live=DEFAULT_TIME_TO_LIVE,
                 **kwargs):
        """ Constructor. """
        super(Message, self).__init__()
        self.source = source
        self.to = to
        self.previous_hop = previous_hop
        self.next_hop = next_hop
        self.command = command
        self.handler = handler
        if isinstance(reply, bool):
            self.kind = MESSAGE_TYPE_REPLY if reply else MESSAGE_TYPE_REQUEST
        else:
            self.kind = reply
        self.payload = dict(kwargs)
        self.time_to_live = time() + time_to_live

    def __str__(self):
        return 'Message(to=%r, src=%r, cmd=%r)' % (
            self.to, self.source, self.command)

    def __repr__(self):
        return \
            'Message(' \
            'source=%r, to=%r, ph=%r, nh=%r, command=%r, ' \
            'kind=%r, payload=%r, ttl=%r)' % (
                self.source,
                self.to,
                self.previous_hop,
                self.next_hop,
                self.command,
                self.kind,
                self.payload,
                self.time_to_live
            )

    def create_reply(self,
                     source=None,
                     to=None,
                     previous_hop=None,
                     next_hop=None,
                     command=None,
                     reply=True,
                     handler=None,
                     time_to_live=DEFAULT_TIME_TO_LIVE,
                     **kwargs):
        """ Creates a reply to the sender of this message. """
        result = Message(
            source=source if source is not None else self.to,
            to=to if to is not None else self.source,
            previous_hop=previous_hop if previous_hop is not None else self.next_hop,
            next_hop=next_hop if next_hop is not None else self.previous_hop,
            command=command if command is not None else self.command,
            reply=reply,
            handler=handler if handler is not None else self.handler,
            **kwargs
        )
        result.time_to_live = time() + time_to_live
        return result

    def encode(self, app_uuid):
        """ Converts the message into a string of bytes suitable
        for transfer. """
        assert self.to is not None
        assert self.command is not None

        if self.next_hop is None:
            self.next_hop = self.to
        if self.source is None:
            self.source = app_uuid

        return \
            self.next_hop, \
            self.source if self.source != app_uuid else b'', \
            self.to if self.to != self.to else b'', \
            bytes([self.kind]), \
            self.command, \
            packb(self.payload)

    @staticmethod
    def parse(raw_data, app_uuid):
        if len(raw_data) != 6:
            logger.error("Received malformed message (%d parts)",
                         len(raw_data))
            logger.debug("Offending message was: %r", raw_data)
            return None

        message = Message(
            next_hop=None,
            previous_hop=raw_data[0],
            source=raw_data[1] if len(raw_data[1]) != 0 else raw_data[0],
            to=raw_data[2] if len(raw_data[2]) != 0 else app_uuid,
            reply=raw_data[3][0],
            command=raw_data[4],
        )
        message.payload = unpackb(raw_data[5])
        return message

    def valid_for_send(self, app):
        """
        Makes sure that this message has required fields for
        sending them by the sender.
        """
        return (
                (self.to is not None) and
                (self.next_hop is not None) and
                (self.source is not None) and
                (self.command is not None) and
                (self.handler is not None) and
                (self.kind is not None) and
                (self.time_to_live is not None) and
                (self.time_to_live >= app.tick)
        )

    @staticmethod
    def validate_messages_for_send(message, app):
        """
        Makes sure that one or more messages have required fields for
        sending them by the sender.
        """
        if isinstance(message, (list, tuple, set)):
            result = True
            for m_one in message:
                result = result and m_one.valid_for_send(app)
        else:
            result = message.valid_for_send(app)
        return result
