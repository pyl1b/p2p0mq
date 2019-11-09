# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

from zmq import Context
from zmq.auth.thread import ThreadAuthenticator

from p2p0mq.constants import TRACE
from .koloopthread import KoLoopThread

logger = logging.getLogger('p2p0mq.thread')


class KoNetThread(KoLoopThread):
    """
    Base thread for network threads.
    """
    def __init__(self, app,
                 bind_address='127.0.0.1', bind_port=None,
                 context=None,
                 no_encryption=False,
                 *args, **kwargs):
        """ Constructor. """
        super(KoNetThread, self).__init__(*args, **kwargs)

        # Where we bind to.
        self.bind_address = str(bind_address)
        self.bind_port = int(bind_port) if bind_port else None

        # The application where we belong to.
        self.app = app

        # The context used to create sockets.
        self.context = context or Context.instance()

        # The socket we use to communicate.
        self.socket = None

        # Set this to disable encryption.
        self.no_encryption = no_encryption

    @property
    def address(self):
        return self.bind_address if self.bind_port is None else \
            "tcp://%s:%d" % (self.bind_address, self.bind_port)

    def create(self):
        """ Called at thread start to initialize the state. """
        pass

    def terminate(self):
        """ Called at thread end to free resources. """
        if self.socket is not None:
            logger.log(TRACE, "Stopping zmq socket with 0 second "
                              "linger on %r app",
                       (self.app.uuid if self.app is not None else ''))
            self.socket.close(0)
            self.socket = None
