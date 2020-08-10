'''
Created on 3 Ağu 2020

@author: boogie
'''
import time
from pony.orm import db_session, set_sql_debug

import tribler_core.utilities.permid as permid_module
from tribler_core.modules.metadata_store.store import MetadataStore
from tribler_core.tests.tools.base_test import TriblerCoreTest
from tribler_core.tests.tools.common import TESTS_DATA_DIR


SAMPLE_DIR = TESTS_DATA_DIR / 'big_channel'
DB_PATH = SAMPLE_DIR / "metadata.db"
CHAN_DIR = SAMPLE_DIR / 'channels'
KEYPAIR = SAMPLE_DIR / 'keychain.pem'

class TestTorrentMetadata(TriblerCoreTest):
    """
    Contains various tests for the torrent metadata type.
    """

    async def setUp(self):
        await super(TestTorrentMetadata, self).setUp()
        self.my_key = permid_module.read_keypair_trustchain(KEYPAIR)
        self.mds = MetadataStore(DB_PATH, CHAN_DIR, self.my_key)
        self.id = "4013912393472139243"
        with db_session:
            set_sql_debug(True, True)
            self.bigchannel = self.mds.ChannelNode.get(id_=self.id)

    async def tearDown(self):
        self.mds.shutdown()
        await super(TestTorrentMetadata, self).tearDown()

    async def test_benchmark_select(self):
        t1 = time.time()
        cursor = await self.bigchannel.iter_nodes_to_commit()
        t2 = time.time()
        print(t2 - t1)
        while 1:
            row =await  self.bigchannel.dbexecutor.fetchone(cursor)
            if not row:
                break
        print(time.time() - t2)
