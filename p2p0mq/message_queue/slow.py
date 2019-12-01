# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.constants import TRACE
from .base import MessageQueue

logger = logging.getLogger('p2p0mq.queue')


class SlowMessageQueue(MessageQueue):
    """
    Queue that sends one message per loop.

    The queue hosts a single list; it puts new messages at the end
    of the list. A single message is chosen on each loop to be send.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(SlowMessageQueue, self).__init__(*args, **kwargs)
        self.queue = []

    def enqueue(self, message):
        """ Adds one or more messages to internal queue to be send later. """
        with self.lock:
            if isinstance(message, (list, tuple, set)):
                self.queue.extend(message)
                logger.log(TRACE, "%d message(s) added to %s: %r",
                           len(message), self, message)
            else:
                self.queue.append(message)
                logger.log(TRACE, "1 message added to %s: %r", self, message)

    def dequeue(self):
        """ Returns a list of messages that should be send. """
        with self.lock:
            result = [self.queue.pop(0)] if len(self.queue) > 0 else []
        logger.log(TRACE, "%d message(s) de-queued from %s: %r",
                   len(result), self, result)
        return result

    def empty(self):
        """Tell if this queue is empty"""
        return len(self.queue) == 0

    def __str__(self):
        return 'SlowMessageQueue(%r)' % len(self.queue)

    def __repr__(self):
        return 'SlowMessageQueue(%r)' % self.queue

    def __len__(self):
        return len(self.queue)
