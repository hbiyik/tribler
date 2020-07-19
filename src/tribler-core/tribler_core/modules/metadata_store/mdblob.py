'''
Created on 1 Aug 2020

@author: boogie
'''
from datetime import datetime
from struct import unpack_from
import lz4.frame

from ipv8.messaging.serialization import default_serializer
from ipv8.keyvault.crypto import default_eccrypto

from tribler_core.modules.metadata_store.discrete_clock import clock
from tribler_core.modules.metadata_store.serialization import (CHANNEL_NODE,
                                                               METADATA_NODE,
                                                               COLLECTION_NODE,
                                                               REGULAR_TORRENT,
                                                               CHANNEL_NODE,
                                                               CHANNEL_TORRENT,
                                                               DELETED)
from tribler_core.modules.metadata_store.serialization import time2int
from numpy import isin
import chunk


TODELETE = 1 # TODO: move those to common

COLUMNFORMATS = {
    "rowid":  (None, None),
    "metadata_type": ("H", 2),
    "reserved_flags": ("H", 2),
    "origin_id": ("Q", 8),
    "public_key": ("64s", 64),
    "id_": ("Q", 8),
    "timestamp": ("Q", 8),
    "signature": ("64s", 64),
    "added_on": (None, None),
    "status": (None, None),
    "title": ("varlenI", 4),
    "tags": ("varlenI", 4),
    "num_entries": ("Q", 8),
    "infohash": ("20s", 20),
    "size": ("Q", 8),
    "torrent_date": ("I", 4),
    "tracker_info": ("varlenI", 4),
    "xxx": (None, None),
    "health": (None, None),
    "start_timestamp": ("Q", 8),
    "subscribed": (None, None),
    "share": (None, None),
    "votes": (None, None),
    "local_version": (None, None),
    }

SERIALIZATIONMAP = {
    CHANNEL_NODE: ("metadata_type", "reserved_flags", "public_key", "id_", "origin_id", "timestamp", "signature"),
    METADATA_NODE: ("metadata_type", "reserved_flags", "public_key", "id_", "origin_id", "timestamp", "title", "tags",
                    "signature"),
    COLLECTION_NODE: ("metadata_type", "reserved_flags", "public_key", "id_", "origin_id", "timestamp", "title", "tags",
                      "num_entries", "signature"),
    REGULAR_TORRENT: ("metadata_type", "reserved_flags", "public_key", "id_", "origin_id", "timestamp", "infohash",
                      "size", "torrent_date", "title", "tags", "tracker_info", "signature"),
    CHANNEL_TORRENT :("metadata_type", "reserved_flags", "public_key", "id_", "origin_id", "timestamp", "infohash",
                      "size", "torrent_date", "title", "tags", "tracker_info", "num_entries", "start_timestamp",
                      "signature"),
    DELETED : ("metadata_type", "reserved_flags", "public_key", "delete_signature", "signature")
    }

ROW_CONVERSIONS = {"torrent_date": lambda x: time2int(datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")),
                   "title": lambda x: x if isinstance(x, bytes) else bytes(x, "utf8"),
                   "tags": lambda x: x if isinstance(x, bytes) else bytes(x, "utf8"),
                   "tracker_info": lambda x: x if isinstance(x, bytes) else bytes(x, "utf8"),
                   }

class MDBlob:
    # class that represents an mdblob as a file object, which inputs row, outputs blob file.
    # this class is optimized for ram usage
    def __init__(self, fname, write=False, timestamp=None, chunksize=None):
        self.fname = fname
        self.f = None
        self.c = None
        self.chunksize = chunksize if chunksize else 40
        self.timestamp =timestamp if timestamp is not None else clock.tick()
        self.__write = write
        self.__record = b""

    async def __aenter__(self):
        if self.__write:
            self.f = self.fname.open("wb")
        else:
            self.f = self.fname.open("rb")
        if self.fname.suffixes == ['.mdblob', '.lz4']:
            if self.__write:
                self.c = lz4.frame.LZ4FrameCompressor(auto_flush=True)
                self.c.__enter__()
                self.f.write(self.c.begin())
            else:
                self.c = lz4.frame.LZ4FrameDecompressor()
                self.c.__enter__()
        return self

    async def __aexit__(self, exception_type, exception, traceback):
        if self.c:
            if self.__write:
                self.f.write(self.c.flush())
            self.c.__exit__(exception_type, exception, traceback)
            self.c = None
        if self.f:
            self.f.__exit__(exception_type, exception, traceback)
            self.f = None

    async def open(self):
        return await self.__enter__()

    async def close(self):
        return await self.__exit__(None, None, None)

    async def write(self, row, private):
        self.timestamp += 1
        record = b""
        isdelete = row.get("status") == TODELETE or row["metadata_type"] == DELETED
        for col in SERIALIZATIONMAP[DELETED if isdelete else row["metadata_type"]]:
            if isdelete and col == "metadata_type":
                colval = DELETED
            elif isdelete and col == "delete_signature":
                # resign delete_sigmnature here or use existing??
                record += default_serializer.pack(COLUMNFORMATS["signature"][0], row["signature"])[0]
                continue
            elif col in ROW_CONVERSIONS:
                colval = ROW_CONVERSIONS[col](row[col])
            elif col == "timestamp" or (row["metadata_type"] == CHANNEL_TORRENT and col == "start_timestamp"):
                colval = self.timestamp
            elif col == "signature":
                record += default_eccrypto.create_signature(private, record)
                break
            else:
                colval = row[col]
            #print("%s,%s,%s,%s" % (col, COLUMNFORMATS[col][0], colval, type(colval)))
            record += default_serializer.pack(COLUMNFORMATS[col][0], colval)[0]
        if self.c:
            record = self.c.compress((record))
        self.f.write(record)

    async def read(self):
        row = {}
        record_offset = 0
        data_index = 0
        metadata_type = None
        data_format, data_size = COLUMNFORMATS["metadata_type"]
        min_record_size = data_size
        resolve_varlen = False
        while True:
            b = self.f.read(self.chunksize)
            if b == b"" and self.__record == b"":
                # eof
                break
            if self.c:
                # decompress
                b = self.c.decompress(b)
            self.__record += b
            if len(self.__record) < min_record_size:
                # chunk is not long enough, read until
                continue
            if resolve_varlen:
                # datasize points to size of varlen, resolve varlen
                # ACHTUNG: We are directly using stuct here, instead we
                # should interact with serialzer instance for future abstraction
                # but for now thats ok
                varlensize = unpack_from('>I', self.__record, record_offset)[0]
                data_size += varlensize
                min_record_size += varlensize
                resolve_varlen = False
                continue
            data = default_serializer.unpack(data_format, self.__record, record_offset)[0]
            if record_offset == 0:
                # first element in the row
                metadata_type = data
            record_offset += data_size
            row[SERIALIZATIONMAP[metadata_type][data_index]] = data
            data_index += 1
            if data_index < len(SERIALIZATIONMAP[metadata_type]):
                # elements in the row
                if SERIALIZATIONMAP[metadata_type][data_index] == "delete_signature":
                    data_format, data_size = COLUMNFORMATS["signature"]
                else:
                    data_format, data_size = COLUMNFORMATS[SERIALIZATIONMAP[metadata_type][data_index]]
                min_record_size += data_size
            else:
                # backlog the rest of record data for new record
                self.__record = self.__record[min_record_size:]
                return row
            if not resolve_varlen and data_format == "varlenI":
                resolve_varlen = True
