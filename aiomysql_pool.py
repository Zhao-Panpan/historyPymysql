#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    async mysql pool
"""

import asyncio
from .aiomysql_connection import connect
from aiomysql import Pool as MysqlPool
from aiomysql.utils import _PoolContextManager


def create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                loop=None, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize, echo=echo,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                       loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo,
                pool_recycle=pool_recycle, loop=loop, **kwargs)
    if minsize > 0:
        async with pool._cond:
            await pool._fill_free_pool(False)
    return pool


class Pool(MysqlPool):
    """
    在原基础上，增加额外的功能
    """

    async def _fill_free_pool(self, override_min):

        # iterate over free connections and remove timeouted ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof() or conn._reader.exception():
                self._free.pop()
                conn.close()

            elif (self._recycle > -1 and self._loop.time() - conn.last_usage > self._recycle):
                self._free.pop()
                conn.close()

            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
