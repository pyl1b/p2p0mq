# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading

logger = logging.getLogger('p2p0mq.queue')


class MessageQueue(object):
    """
    Base class for message queues.

    The class has a lock that governs access to the inner queue
    but leaves the actual backend at the discretion of the
    implementation.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(MessageQueue, self).__init__(*args, **kwargs)

        self.lock = threading.Lock()

    def enqueue(self, message):
        """ Adds a message to internal queue to be send later. """
        raise NotImplementedError

    def dequeue(self):
        """ Returns a list of messages that should be send. """
        raise NotImplementedError

    def empty(self):
        """Tell if this queue is empty"""
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError
