# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading

from p2p0mq.constants import LOOP_END

logger = logging.getLogger('p2p0mq.thread')


class KoThread(threading.Thread):
    """
    A thread that defines  entry, execute and exit.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        threading.Thread.__init__(self, *args, **kwargs)

    def create(self):
        """ Called at thread start to initialize the state. """
        pass

    def terminate(self):
        """ Called at thread end to free resources. """
        pass

    def execute(self):
        """
        Called to execute the main part of the thread.

        In implementations where this function is executed in a loop
        it is expected to return False to break the loop end terminate
        the thread.
        """
        return LOOP_END

    def run(self):
        """ Thread main function. """
        self.create()
        self.execute()
        self.terminate()
