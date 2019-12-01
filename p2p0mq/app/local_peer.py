# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
from time import sleep

import zmq
from zmq.utils.monitor import recv_monitor_message

from ..router import Router
from ..constants import MESSAGE_TYPE_REQUEST, MESSAGE_TYPE_REPLY, TRACE_FUNC, LOOP_CONTINUE, MESSAGE_TYPE_ROUTE, \
    TRACE_NET, STABILIZE_TIMEOUT
from ..concerns.manager import ConcernsManager
from p2p0mq.peer_store import PeerStore
from ..security import SecurityManager
from ..utils.thread.koloopthread import KoLoopThread
from .client import Sender
from .server import Receiver

logger = logging.getLogger('p2p0mq.app')


class LocalPeer(PeerStore, SecurityManager,
                ConcernsManager, Router, KoLoopThread):
    """
    Resource management class that exposes a single local peer.

    This class can be executed both as a thread and as an
    online worker if current thread has nothing to do.
    When used as a thread call `start()` and it will perform
    the necessary steps. When used online call `run()`.

    To see some other router-dealer patterns goto:
    - [http://hintjens.com/blog:42]
    """
    def __init__(self, config,
                 sender_address='127.0.0.1', sender_port=8341,
                 receiver_address='127.0.0.1', receiver_port=8342,
                 zmq_context=None,
                 zmq_monitor=False,
                 *args, **kwargs):
        """ Constructor. """
        super(LocalPeer, self).__init__(*args, **kwargs)
        self.name = 'p2p0mq.A.th' if self.uuid is None \
            else ('%s-p2p0mq.A.th' % self.uuid[-4:-1].decode("utf-8"))

        # Configuration options loaded at startup.
        self.config = config

        # Should we monitor the sockets and log debug information about them?
        self.zmq_monitor = zmq.Poller() if zmq_monitor else None

        # The instrument used for communication.
        self.zmq_context = zmq_context or zmq.Context.instance()

        # The server (receiver) and the client (sender).
        self.receiver = Receiver(
            app=self, context=self.zmq_context,
            bind_address=receiver_address, bind_port=receiver_port)
        self.sender = Sender(
            app=self, context=self.zmq_context,
            bind_address=sender_address, bind_port=sender_port)

        logger.debug('application constructed')

    def create(self):
        """ Starts the local peer. """
        logger.debug("The local peer %r is being created...", self.uuid)
        self.start_db()
        self.prepare_cert_store(self.uuid)
        self.start_auth(self.zmq_context)
        self.start_concerns()
        self.receiver.start()
        self.sender.start()
        logger.debug("The local peer %r was created", self.uuid)

    def terminate(self):
        """ Terminates the local peer.

        This method is not to be called directly by the user of the class.

        This method should be written defensively, as the environment
        might not be fully set (an exception in create() does not prevent
        this method from being executed).
        """
        logger.debug("The local peer %r is being terminated...", self.uuid)

        if self.receiver is not None:
            self.receiver.stop.set()

        if self.sender is not None:
            self.sender.stop.set()

        sleep(0.5)

        if self.receiver is not None:
            self.receiver.join()
            assert self.receiver.socket is None

        if self.sender is not None:
            self.sender.join()
            assert self.sender.socket is None

        self.terminate_concerns()
        self.terminate_auth()
        self.terminate_db()
        logger.debug("The local peers %r was terminated", self.uuid)

    def execute(self):
        """ A single step in the local peer loop. """
        logger.log(TRACE_FUNC, "Application %r starts execute()", self.uuid)
        self.sync_database()

        self.execute_concerns()

        replies = self.process_requests(
            self.receiver.typed_queues[MESSAGE_TYPE_REQUEST])
        requests = self.process_replies(
            self.receiver.typed_queues[MESSAGE_TYPE_REPLY])
        routed = self.process_routes(
            self.receiver.typed_queues[MESSAGE_TYPE_ROUTE])

        self.sender.enqueue_all(replies, requests, routed)
        logger.log(TRACE_FUNC, "Application %r ends execute", self.uuid)

        self.monitor()
        return LOOP_CONTINUE

    def monitor(self):
        """ Called on each loop to do monitoring tasks."""
        if not self.zmq_monitor:
            return

        if len(self.zmq_monitor.sockets) == 0:
            if self.receiver is not None and self.receiver.socket is not None:
                if self.sender is not None and self.sender.socket is not None:
                    self.zmq_monitor.register(
                        self.receiver.socket.get_monitor_socket())
                    self.zmq_monitor.register(
                        self.sender.socket.get_monitor_socket())
                    event_map = {}
                    setattr(self, 'event_map', event_map)
                    # print("Event names:")
                    for name in dir(zmq):
                        if name.startswith('EVENT_'):
                            value = getattr(zmq, name)
                            # print("%21s : %4i" % (name, value))
                            event_map[value] = name

        event_map = getattr(self, 'event_map')
        socket_rec = self.zmq_monitor.sockets[0]
        socket_se = self.zmq_monitor.sockets[1]
        socks = dict(self.zmq_monitor.poll(100))
        if socket_rec in socks and socks[socket_rec] & zmq.POLLIN:
            message = recv_monitor_message(socket_rec)
            message.update({'description': event_map[message['event']]})
            logger.log(TRACE_NET, "RECEIVER: %r", message)
        if socket_se in socks and socks[socket_se] & zmq.POLLIN:
            message = recv_monitor_message(socket_se)
            message.update({'description': event_map[message['event']]})
            logger.log(TRACE_NET, "SENDER: %r", message)

    def stable(self):
        """ Tell if the local peer is started. """
        return (
            self.receiver is not None and
            self.sender is not None and
            self.run_loop_counter > 4 and
            self.receiver.run_loop_counter > 4 and
            self.sender.run_loop_counter > 4 and
            self.next_peer_db_sync_time > self.tick
        )

    def wait_to_stabilize(self):
        """ Wait for the local peer to became stable. """
        for i in range(STABILIZE_TIMEOUT*2):
            if self.stable():
                return True
            sleep(0.5)
        return False

