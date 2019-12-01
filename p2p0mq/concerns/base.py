# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

logger = logging.getLogger('p2p0mq.concern')


class Concern(object):
    """
    Base class for concerns.

    Attributes:
        command_id (bytes):
            A unique command id used as part of the messages initiated
            by this concern. Same id is used both for requests and replies.
        name (str):
            Human readable name of this concern.
        app (ConcernsManager):
            The manager where this concern is installed.
    """
    def __init__(self, name, command_id, app=None, *args, **kwargs):
        """
        Constructor.

        Arguments:
            command_id (bytes):
                A unique command id used as part of the messages initiated
                by this concern. Same id is used both for requests and replies.
            name (str):
                Human readable name of this concern.
            app (ConcernsManager):
                The manager where this concern is installed. As it will
                be set in :meth:`~p2p0mq.concerns.manager.ConcernsManager.add_concern`
                (so available by the time :meth:`~p2p0mq.concerns.base.Concern.start`
                is called), this argument is only useful if initialization
                code needs access to the manager.
        """
        super(Concern, self).__init__(*args, **kwargs)
        self.app = app
        self.command_id = command_id
        self.name = name

    def __str__(self):
        return 'Concern(%r, %r)' % (self.command_id, self.name)

    def start(self):
        """
        Called by the :class:`~p2p0mq.concerns.manager.ConcernsManager`
        to inform the concern that it was installed.

        .. note:

        For concerns installed before the local peer has been started
        this method is called before entering main loop.
        The sender and the receiver are not started at that time.
        """
        pass

    def terminate(self):
        """
        Called by the :class:`~p2p0mq.concerns.manager.ConcernsManager`
        to inform the concern that it was uninstalled.

        At this point main loop has been exited and
        the receiver and the sender have been stopped.
        """
        pass

    def execute(self):
        """ Called from application thread on each thread loop. """
        pass

    def process_request(self, message):
        """
        Handler on the receiver side for requests.

        Arguments:
            message (Message):
                The message that has been received.
        """
        raise NotImplementedError

    def process_reply(self, message):
        """
        Handler on the sender side for replies.

        Arguments:
            message (Message):
                The message that has been received.
        """
        raise NotImplementedError

    def message_sent(self, message):
        """
        We are informed that one of our messages was sent.

        This call is made in the context of the sending thread.

        Arguments:
            message (Message):
                The message that was send.
        """
        pass

    def send_failed(self, message, exc=None):
        """
        We are informed that one of our messages failed to send.

        This call is made in the context of the sending thread
        and only if the time-to-live of the message has not been expired.
        Otherwise, a call to :meth:`~message_dropped` is made.

        By returning the same message the concern essentially implements a
        retry-until-expires mechanism.

        Arguments:
            message (Message):
                The message that failed to send.
            exc (Exception):
                The exception that was raised, if any.

        Returns:
             the message to be re-queued (can be the same message).
             This is NOT a `(PRIORITY, message)` type of reply.
        """
        return None

    def message_dropped(self, message):
        """
        We are informed that one of our messages was dropped.

        This call is made in the context of the sending thread when
        the time-to-live of the message has expired. Unlike
        :meth:`~send_failed`, this method cannot return a message
        to be re-queued.

        Arguments:
            message (Message):
                The message that was send.
        """
        pass
