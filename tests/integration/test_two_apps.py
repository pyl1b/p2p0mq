# -*- coding: utf-8 -*-
"""
Integration test that runs two communicating apps in parallel.
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
import shutil
import tempfile
from time import sleep
from unittest import TestCase, SkipTest
from unittest.mock import MagicMock

import zmq

from p2p0mq.app.local_peer import LocalPeer
from p2p0mq.peer import Peer

LOG_LEVEL_HERE = logging.DEBUG # 1 # logging.WARNING #
logging.getLogger().setLevel(LOG_LEVEL_HERE)
if len(logging.getLogger().handlers) == 0:
    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)-7s] [%(name)-19s] [%(threadName)-15s] "
        "[%(funcName)-25s] %(message)s",
        '%M:%S')
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    logging.getLogger().addHandler(handler)

logging.getLogger('p2p0mq.queue').setLevel(logging.WARNING)
logging.getLogger('p2p0mq.concerns').setLevel(logging.WARNING)
# logging.getLogger('p2p0mq.concern.con').setLevel(1)
# logging.getLogger('p2p0mq.concern.hb').setLevel(1)
# logging.getLogger('p2p0mq.app').setLevel(logging.WARNING)
# logging.getLogger('p2p0mq.app.s').setLevel(1)
# logging.getLogger('p2p0mq.app.c').setLevel(1)


def make_app(tag, no_encryption=True):
    label = '%d~%d~%d~%d' % (tag, tag, tag, tag)
    app = LocalPeer(
        db_file_path=tempfile.mktemp(prefix='ko-%s-db_file_path-' % label),
        private_cert_dir=tempfile.mkdtemp(prefix='ko-%s-private_cert_dir-' % label),
        public_cert_dir=tempfile.mkdtemp(prefix='ko-%s-public_cert_dir-' % label),
        temp_cert_dir=tempfile.mkdtemp(prefix='ko-%s-temp_cert_dir-' % label),
        no_encryption=no_encryption,
        config={},
        sender_address='127.0.0.1',
        sender_port=8300+tag,
        receiver_address='127.0.0.1',
        receiver_port=8400+tag,
        zmq_context=zmq.Context(),
        zmq_monitor=True,
        app_uuid=label
    )
    return app


def end_app(app):

    app.stop.set()
    if app.is_alive():
        app.join()
    app.zmq_context.destroy()
    if os.path.isfile(app.db_file_path):
        os.remove(app.db_file_path)
    if os.path.isdir(app.private_cert_dir):
        shutil.rmtree(app.private_cert_dir)
    if os.path.isdir(app.public_cert_dir):
        shutil.rmtree(app.public_cert_dir)
    if os.path.isdir(app.temp_cert_dir):
        shutil.rmtree(app.temp_cert_dir)


class TestTestee(TestCase):
    def setUp(self):
        self.testee1 = make_app(1, False)
        self.testee2 = make_app(2, False)

    def tearDown(self):
        end_app(self.testee1)
        self.assertIsNone(self.testee1.receiver.socket)
        self.assertIsNone(self.testee1.sender.socket)
        self.testee1 = None

        end_app(self.testee2)
        self.assertIsNone(self.testee2.receiver.socket)
        self.assertIsNone(self.testee2.sender.socket)
        self.testee2 = None

    def test_run(self):

        self.testee1.start()
        self.testee2.start()
        self.testee1.wait_to_stabilize()
        self.testee2.wait_to_stabilize()

        # At this point the two apps are completely isolated.
        # For them to communicate they each need the other's
        # certificates. So let us copy the relevant files from
        # one to the other.
        self.testee1.exchange_certificates(self.testee2)

        self.assertEqual(len(self.testee1.peers), 0)
        self.assertEqual(len(self.testee2.peers), 0)

        # One of the applications creates a peer object and adds it to
        # its list of peers.
        peer = Peer(
            uuid=self.testee2.uuid,
            host=self.testee2.receiver.bind_address,
            port=self.testee2.receiver.bind_port)
        # From there it is picked up by the hart-beat handler and
        # a message is send to the peer. After the proper authentication
        # the peer will create a structure of its own for the incoming peer
        # and acknowledge the message.
        with self.testee1.peers_lock:
            self.testee1.peers[peer.uuid] = peer

        sleep(5)

        self.assertEqual(len(self.testee2.peers), 1)

