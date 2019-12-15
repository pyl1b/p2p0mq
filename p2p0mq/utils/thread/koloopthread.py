# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import threading
from time import time

from p2p0mq.constants import LOOP_CONTINUE
from .kothread import KoThread

logger = logging.getLogger('p2p0mq.thread')


class KoLoopThread(KoThread):
    """
    A thread that implements an inner loop.

    Attributes:
        stop (threading.Event):
            An event that is checked on each thread step. If found set the
            main lop is terminated.
        sleep (threading.Event):
            Event used for sleeping. Instead of using `sleep()` directly
            we use an event that has the added benefit that code in a step
            can prevent the main loop from sleeping.
        tick (float):
            The time stamp for current loop.
        run_loop_counter (int):
            Counts the number of times main loop has been entered. First loop
            will see `run_loop_counter == 0`.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(KoLoopThread, self).__init__(*args, **kwargs)

        # Set this to terminate the thread.
        self.stop = threading.Event()

        # The inner loop sleeps on each loop 0.1 seconds. Methods that
        # Expect some data right away can prevent next loop from sleeping
        # by setting this event.
        self.sleep = threading.Event()

        self.tick = None

        # The index of current loop
        self.run_loop_counter = 0

    def run_counted(self, counter=None, time_limit=None):
        """
        Main thread loop.

        The default implementation for run() will call
        this method without any constraints.

        By using the counter and time_limit parameters the
        caller can set termination conditions oon top of
        the stop signal:
        - the maximum number of loops
        - the time limit.
        """
        logger.debug("Request to run inner thread loop with %r "
                     "counter limit and %r time limit", counter, time_limit)
        result = 0

        # noinspection PyBroadException
        try:
            self.create()

        except Exception:
            logger.critical("Exception while attempting to create thread "
                            "context <%s>; the thread will call terminate() "
                            "and then it will exit", self.name, exc_info=True)
            result = -10

        else:
            while True:
                self.run_loop_counter = self.run_loop_counter + 1

                # Do some soul searching.
                self.sleep.wait(0.1)
                self.sleep.clear()

                # Were we asked to leave?
                if self.stop.is_set():
                    logger.debug("Inner thread loop terminated by stop()")
                    break

                # Save current time.
                self.tick = time()

                # Do some work.
                # noinspection PyBroadException
                try:
                    if self.execute() != LOOP_CONTINUE:
                        logger.debug("Inner thread loop terminated "
                                     "by execute()")
                        result = 3
                        break
                except Exception:
                    logger.critical(
                        "Exception while attempting to execute main thread "
                        "function in <%s>; the thread will call "
                        "terminate() and then it will exit",
                        self.name, exc_info=True)
                    result = -100
                    break

                # Have we reached the loop counts?
                if counter is not None:
                    if self.run_loop_counter >= counter:
                        logger.debug("Inner thread loop terminated by counter "
                                     "limit %r <= %r",
                                     self.run_loop_counter, counter)
                        result = 1
                        break

                # Have we reached the time limit?
                if time_limit is not None:
                    if self.tick >= time_limit:
                        logger.debug("Inner thread loop terminated by time "
                                     "limit %r < %r",
                                     time_limit, self.tick)
                        result = 2
                        break

        # noinspection PyBroadException
        try:
            self.terminate()
        except Exception:
            logger.critical("Exception while attempting to terminate thread "
                            "context <%s>; the thread will now exit",
                            self.name, exc_info=True)
            result = result - 100

        logger.debug("Inner thread loop exits with result %r", result)
        return result

    def run(self):
        """ Thread main function. Simply calls run_counted(). """
        self.run_counted()
