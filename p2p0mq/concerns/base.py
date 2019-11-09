# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

logger = logging.getLogger('p2p0mq.concern')


class Concern(object):
    """
    The concerns deal with messages on both side of the connection.

    Each concern has a command identification and, thus, can only handle
    exactly one command. The parameter is set in the constructor and
    should be kept constant after the instance has been made part of the
    application via start().

    Once the application hs been started the main loop will call execute()
    on the concern on each loop in the context of the app thread.
    The concern is free to add messages to application queue but
    it should not send them directly.

    The application builds maps for concerns so that, when a request or
    reply arrives the appropriate method will be called in the context
    of the sender/receiver thread.
    """
    def __init__(self, app, name, command_id, *args, **kwargs):
        """ Constructor. """
        super(Concern, self).__init__(*args, **kwargs)
        self.app = app
        self.command_id = command_id
        self.name = name

    def __str__(self):
        return 'Concern(%r, %r)' % (self.command_id, self.name)

    def start(self):
        """
        Called from application before entering main loop.

        The sender and the receiver were not started at this point.
        """
        pass

    def execute(self):
        """ Called from application thread on each thread loop. """
        pass

    def terminate(self):
        """
        Called from application thread after main loop has been exited.

        The receiver and the sender have been stopped at this point.
        """
        pass

    def process_request(self, message):
        """ Handler on the receiver side for requests. """
        raise NotImplementedError

    def process_reply(self, message):
        """ Handler on the sender side for replies. """
        raise NotImplementedError

    def send_failed(self, message, exc=None):
        """
        We are informed that one of our messages failed to send.

        This call is made in the context of the sending thread.

        Return the message to be re-queued (can be same message).
        """
        return None

    def message_sent(self, message):
        """
        We are informed that one of our messages was sent.

        This call is made in the context of the sending thread.
        """
        pass

    def message_dropped(self, message):
        """
        We are informed that one of our messages was dropped.

        This call is made in the context of the sending thread.
        """
        pass
