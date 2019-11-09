# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import sqlite3
from unittest import TestCase
from unittest.mock import MagicMock
from time import time, sleep

from p2p0mq.constants import SYNC_DB_INTERVAL
from p2p0mq.db.peer_store import PeerStore, SQLITE_PEERS_TABLE, SQLITE_META_TABLE
from p2p0mq.peer import Peer

logger = logging.getLogger('tests.p2p0mq.db')


class TestKoNetThreadNoDb(TestCase):
    def setUp(self):
        self.testee = PeerStore()

    def test_init(self):
        self.testee = PeerStore()
        self.assertIsNone(self.testee.db_file_path)
        self.assertIsInstance(self.testee.peers, dict)
        self.assertEqual(len(self.testee.peers), 0)
        self.assertIsNotNone(self.testee.next_peer_db_sync_time)
        self.assertIsNone(self.testee.uuid)
        self.assertIsNone(self.testee.db_created)

    def test_table_exists(self):
        cursor = MagicMock()
        name = "ttt"
        cursor.fetchone.return_value = None
        self.assertFalse(self.testee.table_exists(cursor, name))
        cursor.execute.assert_called_once()
        cursor.fetchone.assert_called_once()

        self.testee = PeerStore()
        cursor = MagicMock()
        name = "ttt"
        cursor.fetchone.return_value = []
        self.assertFalse(self.testee.table_exists(cursor, name))
        cursor.execute.assert_called_once()
        cursor.fetchone.assert_called_once()

        self.testee = PeerStore()
        cursor = MagicMock()
        name = "ttt"
        cursor.fetchone.return_value = [1]
        self.assertTrue(self.testee.table_exists(cursor, name))
        cursor.execute.assert_called_once()
        cursor.fetchone.assert_called_once()

    def test_create_peers_table(self):
        cursor = MagicMock()
        self.testee.create_peers_table(cursor)
        cursor.execute.assert_called_once()

    def test_create_meta_table(self):
        self.assertIsNone(self.testee.uuid)
        self.assertIsNone(self.testee.db_created)
        cursor = MagicMock()
        self.testee.create_meta_table(cursor)
        cursor.execute.assert_called_once()
        cursor.executemany.assert_called_once()
        self.assertIsNotNone(self.testee.uuid)
        self.assertIsNotNone(self.testee.db_created)

    def test_sync_database(self):
        self.testee.next_peer_db_sync_time = time()+100
        self.assertIsNone(self.testee.sync_database())


class TestKoNetThreadDb(TestCase):
    def setUp(self):
        self.testee = PeerStore()
        self.testee.db_file_path = "file::memory:?cache=shared"
        self.db = sqlite3.connect(self.testee.db_file_path, uri=True)
        self.cursor = self.db.cursor()

    def tearDown(self):
        self.db.close()

    def test_table_exists(self):
        self.assertFalse(self.testee.table_exists(self.cursor, "ttt"))
        self.cursor.execute("CREATE TABLE ttt (id INTEGER PRIMARY KEY);")
        self.assertTrue(self.testee.table_exists(self.cursor, "ttt"))

    def test_create_metadata(self):
        self.assertFalse(self.testee.table_exists(
            self.cursor, SQLITE_META_TABLE))
        self.testee.create_meta_table(self.cursor)
        self.assertTrue(self.testee.table_exists(
            self.cursor, SQLITE_META_TABLE))
        self.cursor.execute("SELECT * FROM %s ORDER BY key;" % SQLITE_META_TABLE)
        result = self.cursor.fetchall()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], 2)
        self.assertEqual(result[0][1], "db_created")
        sleep(0.1)
        self.assertLess(float(result[0][2]), time())
        self.assertEqual(result[0][3],
                         "the time when the metadata was inserted")
        self.assertEqual(result[1][0], 1)
        self.assertEqual(result[1][1], "uuid")
        sleep(0.1)
        self.assertGreater(len(result[1][2]), 4)
        self.assertEqual(result[1][3],
                         "the unique identification of this instance")

    def test_read_metadata(self):
        self.testee.create_meta_table(self.cursor)
        self.assertFalse(hasattr(self.testee, "ggggg"))
        self.cursor.execute(
            "INSERT INTO %s(key,value,description) VALUES(?, ?, ?);" %
            SQLITE_META_TABLE,
            ("ggggg", "XXX", "description"))
        self.db.commit()
        self.testee.read_metadata()
        self.assertTrue(hasattr(self.testee, "ggggg"))
        self.assertEqual(self.testee.ggggg, "XXX")

    def test_sync_database(self):
        self.assertEqual(len(self.testee.peers), 0)
        self.testee.next_peer_db_sync_time = self.testee.next_peer_db_sync_time - 100
        self.assertIsNone(self.testee.sync_database())
        sync_time = self.testee.next_peer_db_sync_time
        self.assertTrue(self.testee.table_exists(
            self.cursor, SQLITE_PEERS_TABLE))
        self.cursor.execute("SELECT * FROM %s;" %
                            SQLITE_PEERS_TABLE)
        result = self.cursor.fetchall()
        self.assertEqual(len(result), 0)
        self.assertEqual(len(self.testee.peers), 0)

        result = self.testee.sync_database()
        self.assertEqual(len(result), 0)
        self.assertLessEqual(sync_time+SYNC_DB_INTERVAL,
                             self.testee.next_peer_db_sync_time)

        peer = MagicMock(spec=Peer)
        peer.uuid = "111"
        peer.host = "host"
        peer.port = 888
        peer.db_id = None
        self.testee.peers[peer.uuid] = peer

        self.testee.next_peer_db_sync_time = self.testee.next_peer_db_sync_time - 100
        result = self.testee.sync_database()
        self.assertEqual(len(result), 0)
        self.cursor.execute("SELECT * FROM %s;" %
                            SQLITE_PEERS_TABLE)
        result = self.cursor.fetchall()
        self.assertEqual(len(result), 1)
        result = result[0]
        self.assertEqual(result[0], 1)
        self.assertEqual(result[1], "111")
        self.assertEqual(result[2], "host")
        self.assertEqual(result[3], 888)

