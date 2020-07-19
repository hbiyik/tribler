'''
Created on 3 Ağu 2020

@author: boogie
'''

from __future__ import absolute_import

import os
import random
import time
from datetime import datetime
from pathlib import Path

from ipv8.keyvault.crypto import default_eccrypto
from tribler_core.modules.metadata_store.store import MetadataStore
from tribler_core.modules.metadata_store.mdblob import MDBlob, SERIALIZATIONMAP, COLUMNFORMATS
from tribler_core.modules.metadata_store.orm_bindings.channel_metadata import CHANNEL_DIR_NAME_LENGTH
from tribler_core.tests.tools.base_test import TriblerCoreTest
from tribler_core.tests.tools.common import TESTS_DATA_DIR


SAMPLE_DIR = TESTS_DATA_DIR / 'sample_channel'
# Just get the first and only subdir there, and assume it is the sample channel dir
CHANNEL_DIR = [
    SAMPLE_DIR / subdir
    for subdir in os.listdir(SAMPLE_DIR)
    if (SAMPLE_DIR / subdir).is_dir() and len(subdir) == CHANNEL_DIR_NAME_LENGTH
][0]

CHANNEL_METADATA = TESTS_DATA_DIR / 'sample_channel' / 'channel.mdblob'
TESTITERATIONS = [500, 1000, 10000]
# Configure tests paths to test performance on different drive types as above
TESTPATHS = {"5400 RPM": Path("/media/boogie/ARCH-5400/triblertests"),
             "7200 RPM": Path("/media/boogie/ARCH-7200/triblertests"),
             "NVME": Path("/home/boogie/triblertests"),
             "Cheap USB Flash": None,
             }
TESTPATHS ={"Default Disk": CHANNEL_DIR}

def generate_random_rows(count):
    # we generate rows in ram to accurately measure MDBlob Performance
    # recursiveness/ramsize is not a KPI of MDBlob performance, only bytes/sec is

    def randombytefromlen(size):
        return bytes(bytearray(random.getrandbits(8) for _ in range(size)))

    def randomintfromlen(size):
        return int.from_bytes(randombytefromlen(size), "little")

    rows = []
    for _ in range(count):
        metadata_type = random.choice(list(SERIALIZATIONMAP.keys()))
        row = {}
        for col in SERIALIZATIONMAP[metadata_type]:
            if col == "metadata_type":
                row[col] = metadata_type
                continue
            if col == "torrent_date":
                row[col] = "2020-01-01 00:00:00.000000"
                continue
            if col == "signature":
                continue
            if col == "delete_signature":
                col = "signature"
            data_type, data_len = COLUMNFORMATS[col]
            if data_type in ["H", "Q", "I"]:
                data = randomintfromlen(data_len)
            elif data_type in ["varlenI", "64s", "20s"]:
                data = randombytefromlen(255 if data_type == "varlenI" else data_len)
            else:
                raise TypeError
            row[col] = data
        rows.append(row)
    return rows


class TestMDBlob(TriblerCoreTest):
    """
    Contains various tests for the channel metadata type.
    """

    async def setUp(self):
        await super(TestMDBlob, self).setUp()
        self.torrent_template = {"title": "", "infohash": b"", "torrent_date": datetime(1970, 1, 1), "tags": "video"}
        self.my_key = default_eccrypto.generate_key(u"curve25519")
        self.mds = MetadataStore(":memory:", self.session_base_dir, self.my_key)

    async def tearDown(self):
        self.mds.shutdown()
        await super(TestMDBlob, self).tearDown()

    async def test_oldmdblobread(self):
        """
        Test processing a directory containing metadata blobs
        """
        print()
        await self.benchmark_factory(CHANNEL_METADATA, testtag="olddb")
        for fname in CHANNEL_DIR.iterdir():
            await self.benchmark_factory(fname, testtag="olddb")

    async def test_optimumreadchunk(self):
        print()
        for testtag, iswrite, fname, rows in self.itercases([500]):
            if not fname.exists():
                await self.write(fname, generate_random_rows(rows))
            if iswrite:
                continue
            olddeltat = None
            chunksize = 1
            while True:
                rows, deltat = await self.read(fname, chunksize)
                chunksize += 1
                if olddeltat and deltat > olddeltat:
                    self.log("MDBlob Optimum Readsize",
                             rows=rows,
                             compressed=bool(".lz4" in fname.suffixes),
                             optimum=chunksize,
                             name=fname.name,
                             tag=testtag)
                    break
                olddeltat = deltat

    async def test_performance(self):
        print()
        oldrows = None
        for testtag, iswrite, fname, rows in self.itercases(TESTITERATIONS):
            if iswrite:
                if not oldrows == rows:
                    rowobjs =  generate_random_rows(rows)
            else:
                rowobjs = None
            oldrows = rows
            await self.benchmark_factory(fname, rowobjs, testtag)

    def itercases(self, iterations):
        for iswrite in [True, False]:
            for suffix in ["mdblob.lz4", "mdblob"]:
                for testiteration in iterations:
                    for testtag, testpath in TESTPATHS.items():
                        if not testpath:
                            continue
                        if not testpath.exists():
                            os.makedirs(testpath)
                        if iswrite:
                            rows = testiteration
                        else:
                            rows = None
                        fname = "%s.%s" % (testiteration, suffix)
                        yield testtag, iswrite, testpath / fname, rows

    def log(self, *args, **kwargs):
        args = list(args)
        args.extend(["%s %s" % (v, k) for k, v in kwargs.items()])
        print(";".join(args))

    async def read(self, fname, chunksize=None):
        rows = 0
        startts = time.time()
        async with MDBlob(fname, chunksize=chunksize) as blob:
            while True:
                row = await blob.read()
                rows += 1
                if not row:
                    rows -= 1
                    break

        return rows, time.time() - startts

    async def write(self, fname, rows):
        startts = time.time()
        async with MDBlob(fname, True) as blob:
            for row in rows:
                row = await blob.write(row, self.my_key)
        return time.time() - startts

    async def benchmark_factory(self, fname, rows=None, testtag=None):
        iswrite = rows is not None
        if iswrite:
            deltat = await self.write(fname, rows)
            rows = len(rows)
        else:
            rows, deltat = await self.read(fname)
        if testtag:
            filesize = fname.stat().st_size / (1024 * 1024)
            self.log("MDBlob Write Benchmark" if iswrite else "MDBlob Read Benchmark",
                     rows=rows,
                     compressed=bool(".lz4" in fname.suffixes),
                     KRowsPerSec=rows / deltat / 1000,
                     MbytesPerSec=filesize / deltat,
                     name=fname.name,
                     sec=deltat,
                     MB=filesize,
                     tag=testtag,
                     )
