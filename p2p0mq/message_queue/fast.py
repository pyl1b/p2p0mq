# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import random

from p2p0mq.constants import TRACE
from .base import MessageQueue

logger = logging.getLogger('p2p0mq.queue')


class FastMessageQueue(MessageQueue):
    """
    Queue that sends all messages on every loop.

    The queue hosts a single list that is shuffled before sending.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(FastMessageQueue, self).__init__(*args, **kwargs)
        self.queue = []

    def enqueue(self, message):
        """ Adds a message to internal queue to be send later. """
        with self.lock:
            if isinstance(message, (list, set, tuple)):
                self.queue = self.queue + list(message)
                logger.log(TRACE, "%d message(s) added to %s",
                           len(message), self)
            else:
                self.queue.append(message)
                logger.log(TRACE, "1 message added to %s", self)

    def dequeue(self):
        """ Returns a list of messages that should be send. """
        with self.lock:
            to_send = self.queue
            self.queue = []
        random.shuffle(to_send)
        logger.log(TRACE, "%d message(s) de-queued from %s",
                   len(to_send), self)
        return to_send

    def empty(self):
        """Tell if this queue is empty"""
        return len(self.queue) == 0

    def __str__(self):
        return 'FastMessageQueue(%r)' % len(self.queue)

    def __repr__(self):
        return 'FastMessageQueue(%r)' % self.queue

    def __len__(self):
        return len(self.queue)
