# -*- coding: utf-8 -*-
"""

The concerns deal with creation and message handling on both side of
the connection.

Each concern has a command identification and, thus, can only handle
exactly one command-replay pair.
The parameter is set in the constructor and
should be kept constant after the instance has been made part of the
application via :meth:`~p2p0mq.concerns.base.Concern.start`.

Once the application hs been started the main loop will call
:meth:`~p2p0mq.concerns.base.Concern.execute`
on the concern on each loop in the context of the app thread.
The concern is free to add messages to application queue but
it should not send them directly.

The application builds maps for concerns so that, when a request or
reply arrives the appropriate method will be called in the context
of the sender/receiver thread.

.. _concerns_manager:

Manager
-------

Concerns settings and actions are grouped in a
distinct class - :class:`~p2p0mq.concerns.manager.ConcernsManager`.
Current implementation merges the manager into
the :ref:`application class<top_level_management>`.

The class stores a dictionary of active concerns indexed by their
command id (NOT the name). Some build-in concerns are automatically added
via :meth:`~p2p0mq.concerns.manager.ConcernsManager.add_all_library_concerns`
and other concerns should be added using
:meth:`~p2p0mq.concerns.manager.ConcernsManager.add_concern`.

The manager informs the :class:`~p2p0mq.concerns.base.Concern`
that it is entering active duty by calling is
:meth:`~p2p0mq.concerns.base.Concern.start` method either when the
application is starting or, if added later, at
:meth:`~p2p0mq.concerns.manager.ConcernsManager.add_concern` time.

Initialization of the manager through
:meth:`~p2p0mq.concerns.manager.ConcernsManager.start_concerns`
adds some (:class:`~p2p0mq.concerns.heart_beat.HeartBeatConcern',
:class:`~p2p0mq.concerns.connector.ConnectorConcern')
build-in concerns ONLY if the list is empty. Otherwise, it
is the responsibility of the caller to populate the list, including
the build in concerns. :meth:`~p2p0mq.concerns.base.Concern.start`
method of each concern in the list will be invoked at this time.

Termination of the manager is equally simple, with
:meth:`~p2p0mq.concerns.base.Concern.terminate` method of each
concern being called.

In the context of the application thread, on each loop, the manager
participates with:

* :meth:`~p2p0mq.concerns.manager.ConcernsManager.execute_concerns` which \
calls the :meth:`~p2p0mq.concerns.base.Concern.execute` method of \
every concern it knows about; the concerns thus have the ability to \
generate messages spontaneously;
* :meth:`~p2p0mq.concerns.manager.ConcernsManager.process_requests` for \
messages directed at local peer and
* :meth:`~p2p0mq.concerns.manager.ConcernsManager.process_replies` for \
replies to messages initiated by the local peer.

Message processing methods (both requests and replies) take their
input from application level queues. On each loop a number of messages
(not larger than `PROCESS_LIMIT_PER_LOOP`) will be de-queued,
the corresponding :class:`~p2p0mq.concerns.base.Concern` will be
located and asked to :meth:`~p2p0mq.concerns.base.Concern.process_reply`
or to :meth:`~p2p0mq.concerns.base.Concern.process_request`. Either
can return a message which the application will enqueue (this is just
a convenience feature; the concern can enqueue the messages itself
and return `None`).

.. _concerns_class:

A Concern
---------

The interface for concerns is defined in :class:`~p2p0mq.concerns.base.Concern`.
Users of the library will often create subclasses of it to implement
new types of messages.

The :class:`~p2p0mq.concerns.base.Concern` is informed about
events regarding its own life (:meth:`~p2p0mq.concerns.base.Concern.start`
and :meth:`~p2p0mq.concerns.base.Concern.terminate`) and about the
life of the messages it handles (
:meth:`~p2p0mq.concerns.base.Concern.message_sent`,
:meth:`~p2p0mq.concerns.base.Concern.send_failed`,
:meth:`~p2p0mq.concerns.base.Concern.message_dropped`).

Its main functions are:

* to generate messages, either from \
:meth:`~p2p0mq.concerns.base.Concern.execute` (called on each thread loop) \
or by using new methods (include `compose` in the name of the method \
for consistency);
* to reply to messages in \
:meth:`~p2p0mq.concerns.base.Concern.process_request`;
* to handle message responses in \
:meth:`~p2p0mq.concerns.base.Concern.process_reply`.

.. _concerns_build_in:

Build-in Concerns
-----------------

.. _connector_concern:

Connector
^^^^^^^^^

The purpose of this concern is to monitor the list of peers for new entries
and to attempt to establish a connection with them.

For peers in initial state (those we never attempted to
connect before) that also have a host set we create a
message where we also set our connection parameters.

The INITIAL state of the peer becomes CONNECTING only when we are
informed in :meth:`~p2p0mq.concerns.connector.ConnectorConcern.message_sent`
that we were able to send the message. If we get
a failure via :meth:`~p2p0mq.concerns.connector.ConnectorConcern.send_failed` or
:meth:`~p2p0mq.concerns.connector.ConnectorConcern.message_dropped` the
state of the peer is set to NO_CONNECTION.

For peers in CONNECTING state (message sent but reply did not arrive),
it the timeout has been exceeded, we also set the NO_CONNECTION state.

When we receive a connect request the state is updated for originating peer
to either CONNECTED or ROUTED, depending on the path it arrived on. Same thing
happens when the reply to a previous request is received. The peer is left
in a state appropriate for the heart-beat to pickup.

.. _heartbeat_concern:

Heart-beats
^^^^^^^^^^^

The purpose of this concern is to monitor connectivity status for each peer
in the list by regularly sending them small messages and measuring
the time it takes to get back.

Using the :meth:`~p2p0mq.concerns.heart_beat.HeartBeatConcern.execute`
hook the concern monitors the state of the peers which have
three relevant members:

* *next_heart_beat_time*: at this time (in seconds since Epoch) \
a heart beat request will be sent if nothing resets the timer until then;
* *last_heart_beat_time*: records the last time a message has been seen \
from this particular peer and is used to change the state of the peer\
to UNRESPONSIVE (if UNRESPONSIVE_THRESHOLD second have passed) or to \
NO_CONNECTION (if NO_CONNECTION_THRESHOLD have passed).
* *slow_heart_beat_down*: when *next_heart_beat_time* is reached \
a message is send and *slow_heart_beat_down* is increased by \
HEART_BEAT_SLOW_DOWN seconds, so each time it increases \
the time between two consecutive heart-beat requests \
(see :meth:`~p2p0mq.peer.Peer.schedule_heart_beat`).

.. note:
   In UNRESPONSIVE state only heart-beat messages are allowed to be sent.

Ask-Around
^^^^^^^^^^^

Used for peers that we don't know. See :ref:`routing` for details about
its workings.

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

    Attributes:
        concerns (dict):
            The list of :class:`concerns <p2p0mq.concerns.base.Concern>`
            we know about, indexed by their `command_id`.
        concerns_started (bool):
            Flag to tell if the start method has been called.
            This is used to determine who's responsibility it is to
            call :meth:`~p2p0mq.concerns.base.Concern.start` on
            newly added concerns.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(ConcernsManager, self).__init__(*args, **kwargs)

        # These are plugins that are hooked up into the application events.
        self.concerns = {}
        self.concerns_started = False

    def add_concern(self, concern):
        """
        Adds a single concern to the list.

        Arguments:
            concern (Concern):
                The new concern to add. It is asserted that the command id is
                not present in the dictionary.
        """
        assert concern.command_id not in self.concerns
        self.concerns[concern.command_id] = concern
        concern.app = self
        if self.concerns_started:
            concern.start()

    def add_all_library_concerns(self):
        """
        Creates instances of some of the concerns defined in this package
        and adds them to the list.

        The concerns added are:

        * :class:`~p2p0mq.concerns.heart_beat.HeartBeatConcern`
        * :class:`~p2p0mq.concerns.connector.ConnectorConcern`

        The concerns defined in this package but not included by default are:

        * :class:`~p2p0mq.concerns.ask_around.AskAroundConcern`
        """
        self.add_concern(HeartBeatConcern(self))
        self.add_concern(ConnectorConcern(self))

    def start_concerns(self):
        """
        Called by the application code at startup time to install hooks.

        Adding concerns after this After this point the list should not be changed.
        """
        if len(self.concerns) == 0:
            self.add_all_library_concerns()

        for concern in self.concerns.values():
            logger.debug("Concern %s is being started", concern)
            concern.start()
        self.concerns_started = True
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

    def execute_concerns(self):
        """
        Execute concerns.

        Called on each execute step by the application.
        Call each concern's execute method in turn.
        """
        for concern in self.concerns.values():
            concern.execute()

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
