# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from p2p0mq.constants import HEART_BEAT_INTERVAL, HEART_BEAT_MAX_INTERVAL, HEART_BEAT_SLOW_DOWN

logger = logging.getLogger('p2p0mq.peer')


# The peer was created but no connection attempt has ben made.
INITIAL = 1
# We connected the socket and we have send the hello message.
CONNECTING = 2
# The hello packet was acknowledged.
# The heart beat is returned in a timely fashion.
CONNECTED = 3
# We cannot connect to this peer directly but we can
# send messages to it by using the via parameter.
ROUTED = 4
# The heart beat dis not returned in time. We were at some
# point in the past connected to this peer.
UNREACHABLE = -1
# We attempted a connection to this peer and we failed.
# Unreachable peers will decay to this state after some time.
NO_CONNECTION = -2


class Peer(object):
    """
    A peer represented in our application.
    """
    def __init__(self, uuid=None, host=None, port=None, db_id=None):
        """ Constructor. """
        super(Peer, self).__init__()
        self.uuid = uuid
        self.host = host
        self.port = port
        self.db_id = db_id
        self.conn_state = INITIAL
        self.via = None

        # Indicates the responsiveness of the peer.
        self.next_heart_beat_time = None
        self.last_heart_beat_time = None
        self.slow_heart_beat_down = 0

        # Don't ask too often about missing peers.
        self.next_ask_around_time = None
        self.last_ask_around_time = None

    def __str__(self):
        return "Peer(%r)" % self.uuid

    def __repr__(self):
        return "Peer(uuid=%r, host=%r, port=%r, db_id=%r, nhbt=%r, lhbt=%r, " \
               "state=%r, via=%r)" % (
            self.uuid, self.host, self.port, self.db_id,
            self.next_heart_beat_time, self.last_heart_beat_time,
            self.state, self.via
        )

    def __hash__(self):
        return hash(self.uuid)

    @property
    def address(self):
        return self.host if self.port is None else 'tcp://%s:%d' % (
            self.host, self.port)

    @property
    def state_initial(self):
        """ The peer was created but no connection attempt has ben made. """
        return self.conn_state == INITIAL

    @state_initial.setter
    def state_initial(self, value):
        """ The peer was created but no connection attempt has ben made. """
        self.conn_state = INITIAL

    @property
    def state_connecting(self):
        """ We connected the socket and we have send the hello message. """
        return self.conn_state == CONNECTING

    @state_connecting.setter
    def state_connecting(self, value):
        """ We connected the socket and we have send the hello message. """
        self.conn_state = CONNECTING

    @property
    def state_connected(self):
        """ The hello packet was acknowledged. The heart beat is returned in
        a timely fashion. """
        return self.conn_state == CONNECTED

    @state_connected.setter
    def state_connected(self, value):
        """ The hello packet was acknowledged. The heart beat is returned in
        a timely fashion. """
        self.conn_state = CONNECTED

    @property
    def state_routed(self):
        """ We cannot connect to this peer directly but we can
        send messages to it by using the via parameter. """
        return self.conn_state == ROUTED

    @state_routed.setter
    def state_routed(self, value):
        """ We cannot connect to this peer directly but we can
        send messages to it by using the via parameter. """
        self.conn_state = ROUTED

    @property
    def state_unreachable(self):
        """ The heart beat dis not returned in time. We were at some
        point in the past connected to this peer. """
        return self.conn_state == UNREACHABLE

    @state_unreachable.setter
    def state_unreachable(self, value):
        """ The heart beat dis not returned in time. We were at some
        point in the past connected to this peer. """
        self.conn_state = UNREACHABLE

    @property
    def state_no_connection(self):
        """ We attempted a connection to this peer and we failed.
        Unreachable peers will decay to this state after some time. """
        return self.conn_state == NO_CONNECTION

    @state_no_connection.setter
    def state_no_connection(self, value):
        """ We attempted a connection to this peer and we failed.
        Unreachable peers will decay to this state after some time. """
        self.conn_state = NO_CONNECTION

    @property
    def state(self):
        return Peer.state_to_string(self.conn_state)

    @staticmethod
    def state_to_string(state):
        if state == INITIAL:
            return 'INITIAL'
        elif state == CONNECTED:
            return 'CONNECTED'
        elif state == ROUTED:
            return 'ROUTED'
        elif state == UNREACHABLE:
            return 'UNREACHABLE'
        elif state == NO_CONNECTION:
            return 'NO CONNECTION'
        else:
            raise ValueError

    @property
    def needs_reconnect(self):
        """ Tell if this peer should be reconnected."""
        return self.conn_state in (INITIAL, NO_CONNECTION, UNREACHABLE)

    @property
    def does_heart_beat(self):
        """ Tell if this peer is a valid destination for a
        heart-beat based on its state."""
        return self.conn_state in (CONNECTED, ROUTED, UNREACHABLE)

    def reset_heart_beat(self, app):
        self.next_heart_beat_time = app.tick + HEART_BEAT_INTERVAL
        self.slow_heart_beat_down = 0
        self.last_heart_beat_time = app.tick

    def schedule_heart_beat(self, app):
        self.next_heart_beat_time = \
            app.tick + HEART_BEAT_INTERVAL + self.slow_heart_beat_down
        self.slow_heart_beat_down = \
            min(self.slow_heart_beat_down + HEART_BEAT_SLOW_DOWN,
                HEART_BEAT_MAX_INTERVAL)

    def become_connected(self, direct, via, app):
        if direct:
            self.state_connected = True
            self.via = None
            logger.debug("%s is now a direct connection", self)
        else:
            self.state_routed = True
            self.via = via
            logger.debug("%s is now a proxied connection", self)
        self.reset_heart_beat(app)
