# -*- coding: utf-8 -*-
"""
Thi is an override module that is useful for debugging.
"""

import logging

from p2p0mq.constants import (
    TRACE_FUNC, TRACE_NET, TRACE, APP_NAME
)


def init_by_logger_kafka(app, args):
    pass


def init(args):
    KafkaHandler.install(args)


def init_by_logger(args):
    p2p0mq_logger = logging.getLogger('p2p0mq')
    p2p0mq_db_logger = logging.getLogger('p2p0mq.db')
    p2p0mq_db_watcher_logger = logging.getLogger('p2p0mq.db.watcher')
    p2p0mq_app_watcher_logger = logging.getLogger('p2p0mq.app')

    p2p0mq_logger.setLevel(1)
    p2p0mq_db_logger.setLevel(1)
    p2p0mq_db_watcher_logger.setLevel(1)
    p2p0mq_app_watcher_logger.setLevel(1)


class KafkaHandler(logging.StreamHandler):
    # record.args
    # record.asctime
    # record.created
    # record.exc_info
    # record.exc_text
    # record.filename
    # record.funcName
    # record.levelname
    # record.levelno
    # record.lineno
    # record.message
    # record.module
    # record.msecs
    # record.msg
    # record.name
    # record.pathname
    # record.process
    # record.processName
    # record.relativeCreated
    # record.stack_info
    # record.thread
    # record.threadName
    def __init__(self, args):
        self.args = args
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        super().emit(record)

        # if self.args.app_port == 5442:
        #     if record.threadName in ('thTacoServer', 'thTacoSeMon', 'thTacoServerAuth'):
        #         if 'cycle start with 0 active sockets' not in msg:
        #             super().emit(record)
        # elif self.args.app_port == 5440:
        #     if record.threadName in ('thTacoClient', 'thTacoClMon', 'thTacoClientAuth'):
        #         super().emit(record)

    @staticmethod
    def install(args):
        # The format we're going to use with console output.
        fmt = logging.Formatter(
            "[%(asctime)s] [%(levelname)-7s] [%(name)-19s] "
            "[%(threadName)-15s] "
            "[%(funcName)-25s] %(message)s",
            '%M:%S')

        logger = logging.getLogger(APP_NAME)
        logger.handlers = []
        logger = logging.getLogger('')
        logger.handlers = []

        console_handler = KafkaHandler(args)
        console_handler.setFormatter(fmt)
        console_handler.setLevel(1)
        logger.addHandler(console_handler)
        logger.setLevel(1)
