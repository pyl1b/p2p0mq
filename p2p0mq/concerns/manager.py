# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.concerns.connector import ConnectorConcern
from p2p0mq.constants import TRACE_NET, SPEED_SLOW, SPEED_MEDIUM, SPEED_FAST, PROCESS_LIMIT_PER_LOOP, ISOLATE, TRACE
from .heart_beat import HeartBeatConcern

logger = logging.getLogger('p2p0mq.concerns')


class ConcernsManager(object):
    """
    Manages the concerns inside the application.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(ConcernsManager, self).__init__(*args, **kwargs)

        # These are plugins that are hooked up into the application events.
        self.concerns = {}

    def add_concern(self, concern):
        """ Adds a single concern to the list. """
        assert concern.command_id not in self.concerns
        self.concerns[concern.command_id] = concern

    def add_all_library_concerns(self):
        """ Creates instances of the concerns defined in this package
        and adds them to the list. """
        self.add_concern(HeartBeatConcern(self))
        self.add_concern(ConnectorConcern(self))

    def start_concerns(self):
        """
        Called by the application code at startup time to install hooks.

        After this point the list should not be changed.
        """
        if len(self.concerns) == 0:
            self.add_all_library_concerns()

        for concern in self.concerns.values():
            logger.debug("Concern %s is being started", concern)
            concern.start()
        logger.debug("All concerns (%d) were started", len(self.concerns))

    def terminate_concerns(self):
        """
        Called by the application code when the application ends.

        This method should be written defensively, as the environment
        might not be fully set (an exception in create() does not prevent
        this method from being executed).
        """
        for concern in self.concerns.values():
            logger.debug("Concern %s is being terminated", concern)
            concern.terminate()
        logger.debug("All concerns (%d) were terminated", len(self.concerns))

    def process_requests(self, queue):
        """
        Called on the application thread to process requests.

        Requests are received by the server (Receiver) and are simply
        deposited in the queue. This function takes the requests and
        delivers them to concern handlers.
        """
        return self.process_common(queue, 'request', False)

    def process_replies(self, queue):
        """
        Called on the application thread to process replies.

        Replies are received by the server (Receiver) and are simply
        deposited in the queue. This function takes the replies and
        delivers them to concerned handlers.
        """
        return self.process_common(queue, 'reply', True)

    def process_common(self, queue, label, reply):
        """
        Called on the application thread to process requests and replies.
        """
        logger.log(TRACE, "Processing %s queue", label)

        results = {
            SPEED_SLOW: [],
            SPEED_MEDIUM: [],
            SPEED_FAST: [],
        }

        for i in range(PROCESS_LIMIT_PER_LOOP):
            messages = queue.dequeue()
            if len(messages) == 0:
                logger.log(TRACE, "No %s to process; early exit", label)
                break

            for message in messages:
                logger.log(TRACE,
                           "concerns handler received %s: %r",
                           label, message)

                # Locate the concern.
                try:
                    concern = self.concerns[message.command]
                except KeyError:
                    logger.error("Received unknown %s %r",
                                 label, message.command)
                    logger.debug("Offending message was: %r", message)
                    continue
                message.handler = concern

                # Call the concern's handler.
                # noinspection PyBroadException
                try:
                    logger.log(TRACE, "Call the concern's handler")

                    if reply:
                        result = concern.process_reply(message)
                    else:
                        result = concern.process_request(message)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception:
                    logger.error("Exception while processing %s %r",
                                 label, message.command)
                    logger.debug("Offending message was: %r",
                                 message, exc_info=True)
                    continue

                # We can send the message if there is one.
                if result is None:
                    logger.log(TRACE_NET,
                               "no response will be send for this %s",
                               label)
                    continue

                priority, result = result
                logger.log(TRACE_NET,
                           "response send for this %s will be %r",
                           label, result)

                results[priority].append(result)

        return results

