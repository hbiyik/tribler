'''
Created on 10 Ağu 2020

@author: boogie
'''

import sqlite3
from asyncio import get_event_loop
import concurrent.futures

from pony.orm import db_session


class DBExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, db):
        super(DBExecutor, self).__init__(max_workers=1)
        self.__db = db
        self.__con = None
        self.__cur = None

    def __getcon(self):
        db_session()._enter()
        cache = self.__db._get_cache()
        self.__con = cache.prepare_connection_for_query_execution()
        self.__con.row_factory = sqlite3.Row

    def __wrapper(self, callback, *args):
        return callback(*args)

    async def __thread(self, callback, *args):
        return await get_event_loop().run_in_executor(self, callback, *args)

    async def con(self):
        if not self.__con:
            await get_event_loop().run_in_executor(self, self.__getcon)
        return self.__con

    async def cur(self, con=None):
        if not con:
            self.__cur = await self.__thread(self.__con.cursor)
            return self.__cur
        return await self.__thread(con.cursor)

    async def execute(self, sql, *args, cur=None):
        if not cur:
            await self.con()
            await self.cur()
            cur = self.__cur
        await self.__thread(cur.execute, sql, *args)
        return cur

    async def fetchone(self, cur=None):
        if not cur:
            cur = self.__cur
        return await self.__thread(cur.fetchone)
