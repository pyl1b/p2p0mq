# -*- coding: utf-8 -*-
"""
"""
import logging

__author__ = "Nicu Tofan"
__package_name__ = "p2p0mq"
__copyright__ = "Copyright 2019, Nicu Tofan"
__credits__ = []
__license__ = "MIT"
__maintainer__ = "Nicu Tofan"
__email__ = "nicu.tofan@gmail.com"


# ---- Logging constants ----
TRACE_PACKETS = 51
TRACE_FUNC = 2
TRACE_NET = 3
TRACE = 4
ISOLATE = 51
logging.addLevelName(TRACE_FUNC, "-PACK")
logging.addLevelName(TRACE_FUNC, "TFUNC")
logging.addLevelName(TRACE_NET, "TNET")
logging.addLevelName(TRACE, "TRACE")
logging.addLevelName(ISOLATE, "ISOLATE")

# ---- Heart Beat ----
# Time between two heart-beat messages.
HEART_BEAT_INTERVAL = 4
# Increase in time interval  between two heart-beat messages
# for unresponsive peers.
HEART_BEAT_SLOW_DOWN = 2
# Longest time interval between two heart beats.
HEART_BEAT_MAX_INTERVAL = 10
# Time interval after last heart-beat when the peer is declared unresponsive.
UNRESPONSIVE_THRESHOLD = HEART_BEAT_INTERVAL*10
# Time interval after last heart-beat when the peer is declared disconnected.
NO_CONNECTION_THRESHOLD = 240

# ---- Other Time Constants ----
# Time between consecutive database sync calls.
SYNC_DB_INTERVAL = 10
# Time between two ask-arounds for the same peer.
ASK_AROUND_INTERVAL = 60
# The time a message is kept in buffers before it is discarded.
DEFAULT_TIME_TO_LIVE = 30

# ---- Speed constants for message queues ----
SPEED_SLOW = -1
SPEED_MEDIUM = 0
SPEED_FAST = 1

# ---- The types of messages ----
MESSAGE_TYPE_REQUEST = 1
MESSAGE_TYPE_REPLY = 2
MESSAGE_TYPE_ROUTE = 3

# ---- Loop commands ----
LOOP_CONTINUE = True
LOOP_END = False

# ---- Loop limits ----
# The maximum number of messages the sender receives in a loop.
RECEIVE_LIMIT_PER_LOOP = 10
# The maximum number of messages the application processes in a loop.
PROCESS_LIMIT_PER_LOOP = RECEIVE_LIMIT_PER_LOOP + 2
