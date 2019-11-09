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


class RegularMessageQueue(MessageQueue):
    """
    Queue that sends one message from all peers on each loop.

    The queue hosts a list for each peer; it puts new messages at the end
    of the list.

    Oldest message is send on each loop and the order among peers
    is randomized.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(RegularMessageQueue, self).__init__(*args, **kwargs)
        self.queue = {}
        self._messages_count = 0

    def enqueue(self, message):
        """ Adds a message to internal queue to be send later. """
        if not isinstance(message, (list, set, tuple)):
            message = [message]

        with self.lock:
            for msg in message:
                try:
                    peer_queue = self.queue[msg.to]
                except KeyError:
                    peer_queue = []
                    self.queue[msg.to] = peer_queue
                peer_queue.append(msg)
            self._messages_count = self._messages_count + len(message)
        logger.log(TRACE, "%d message(s) added to %s", len(message), self)

    def dequeue(self):
        """ Returns a list of messages that should be send. """
        with self.lock:
            result = [
                queue.pop(0) for queue in self.queue.values()
                if len(queue) > 0]
            random.shuffle(result)
            self._messages_count = self._messages_count - len(result)
            logger.log(TRACE, "%d message(s) de-queued from %s",
                       len(result), self)
            return result

    def empty(self):
        """ Tell if this queue is empty. """
        return self._messages_count == 0

    def __str__(self):
        return 'RegularMessageQueue(peers=%r, messages=%r)' % (
            len(self.queue), self._messages_count)

    def __repr__(self):
        return 'RegularMessageQueue(%r)' % self.queue

    def __len__(self):
        return self._messages_count
