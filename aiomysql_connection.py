#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    async mysql connection
"""

import re
import getpass
from datetime import datetime
from pymysql.converters import decoders
from aiomysql.cursors import Cursor, _DeserializationCursorMixin, _DictCursorMixin
from aiomysql.utils import _ConnectionContextManager, _ContextManager
from aiomysql.log import logger
from aiomysql import Connection as AioMysqlConnection
from aiomysql.cursors import Cursor as AioMysqlCursor
from pymysql.err import NotSupportedError, ProgrammingError

from .parse_common import ParseSQL, DMLType

DEFAULT_USER = getpass.getuser()

RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\b.+\bVALUES?\s*)" +
    r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
    r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL)


class Connection(AioMysqlConnection):
    def __init__(self, *arg, postfix='_history', base_column=None, operate_history=False, **kwarg):
        """
        :param arg:
        :param postfix: the history table's postfix
        :param operate_history: whether to operate history table
        :param cursorclass:
        :param kwarg:
        """
        self.postfix = postfix
        self.operate_history = operate_history
        self.base_column = base_column
        self.history_cursor_class = None
        super().__init__(*arg, **kwarg)

    def cursor(self, *cursors, operate_user=None):
        """Instantiates and returns a cursor

        By default, :class:`Cursor` is returned. It is possible to also give a
        custom cursor through the cursor_class parameter, but it needs to
        be a subclass  of :class:`Cursor`

        :param cursors: custom cursor class.
        :param operate_user: history table operate user.
        :returns: instance of cursor, by default :class:`Cursor`
        :raises TypeError: cursor_class is not a subclass of Cursor.
        """
        if operate_user:
            assert isinstance(operate_user, str), "operate user field must be a string."
        self._ensure_alive()
        self._last_usage = self._loop.time()
        try:
            if cursors and \
                    any(not issubclass(cursor, Cursor) for cursor in cursors):
                raise TypeError('Custom cursor must be subclass of Cursor')
        except TypeError:
            raise TypeError('Custom cursor must be subclass of Cursor')
        if cursors and len(cursors) == 1:
            self.history_cursor_class = cursors[0]
            cur = cursors[0](self.postfix, self.operate_history, self.base_column, operate_user, self, self._echo)
        elif cursors:
            cursor_name = ''.join(map(lambda x: x.__name__, cursors)) \
                              .replace('Cursor', '') + 'Cursor'
            cursor_class = type(cursor_name, cursors, {})
            self.history_cursor_class = cursor_class
            cur = cursor_class(self.postfix, self.operate_history, self.base_column, operate_user, self, self._echo)
        else:
            cur = self.cursorclass(self.postfix, self.operate_history, self.base_column, operate_user, self, self._echo)
        fut = self._loop.create_future()
        fut.set_result(cur)
        return _ContextManager(fut)


class Cursor(AioMysqlCursor):
    def __init__(self, history_posix, history_operate, base_column, record_operate_user, *arg, **kwargs):
        self.history_additional_cols = ['record_begin_time', 'record_end_time', 'record_operate_user']
        self.history_operate = history_operate
        self._history_posix = history_posix
        self.base_column = base_column
        self._record_end_time = '9999-12-31'
        self._record_operate_user = record_operate_user if record_operate_user else ""
        self.pairs = None
        super().__init__(*arg, **kwargs)

    def _get_history_cursor(self):
        """
        Create a new cursor to execute queries other than users
        :return:
        """
        self.connection._ensure_alive()
        self.connection._last_usage = self.connection._loop.time()
        cursor = AioMysqlCursor(self.connection, self.connection._echo)
        fut = self.connection._loop.create_future()
        fut.set_result(cursor)
        return _ContextManager(fut)

    async def _extract_table_column(self, table_name: str) -> list:
        """
        extract the column from table
        :param table_name:
        :return: the column list
        """
        sql = "select column_name from information_schema.columns where table_name = %s and table_schema = '{}'".format(
            self._get_db().db)
        cursor = await self._get_history_cursor()
        await cursor.execute(sql, [table_name])
        col_names = await cursor.fetchall()
        ret = [col_name[0] for col_name in col_names]
        await cursor.close()
        return ret

    async def _execute_history_dml(self, sql):
        """
        execute dml sql except other than query statement and users
        :param sql:
        :param args:
        :return:
        """
        cursor = await self._get_history_cursor()
        await cursor.execute(sql)
        await cursor.close()

    async def _execute_history_query(self, sql, args):
        """
        execute query statement other than users
        :param sql:
        :param args:
        :return:
        """
        cursor = await self._get_history_cursor()
        await cursor.execute(sql, args)
        ret = await cursor.fetchall()
        await cursor.close()
        return ret

    async def _query_record_pk(self, sql, args, args_many, cols):
        args = args if args_many else [args]
        ret = []
        if self.connection.history_cursor_class:
            col_li = cols.split(',')
            cursor = self.connection.history_cursor_class(None, False, None, None, self.connection, self.connection._echo)
            for arg in args:
                await cursor._origin_execute(sql, arg)
                ret += [[r.get(col) or r.get('id') for col in col_li] for r in await cursor.fetchall()]
            await cursor.close()
        else:
            for arg in args:
                ret += list(await self._execute_history_query(sql, arg))
        return ret

    async def _process_insert(self, stream, query, args, args_many=False):
        table_name = stream.extract_insert_table()
        if not table_name:
            return None
        ret = (await self._origin_executemany(query, args)) if args_many else (await self._origin_execute(query, args))
        if args_many:
            ids = self._get_insert_ids()
        else:
            first_row_id, row_cnt = self.lastrowid, self.rowcount
            ids = ','.join([str(i) for i in range(first_row_id, first_row_id + row_cnt)])
        col_name = [name for name in await self._extract_table_column(table_name) if name not in self.base_column]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await self._insert_history_record(table_name, col_name, ids, current_time)
        return ret

    def get_insert_ids(self):
        """
        获取 insert 操作所有的id 值
        :return:
        """
        ids = list()
        for lri, ct in self.pairs:
            for i in range(ct):
                c = lri + i
                ids.append(c)
        return ids

    def _get_insert_ids(self, sep=","):
        """
        获取insert操作的id并且将这些id按逗号拼接
        :return:
        """
        ids = list()
        for lri, ct in self.pairs:
            for i in range(ct):
                c = lri + i
                ids.append(str(c))
        return "{}".format(sep).join(ids)

    async def _get_table_from_update_info(self, alias_li, column_li, table_alias_mapping):
        table_cols_mapping = {table: await self._extract_table_column(table) for table in table_alias_mapping.values()}
        if column_li and len(table_alias_mapping.values()) == 1:
            table_li = [list(table_alias_mapping.values())[0]]
        elif column_li:
            table_li = [table_alias_mapping.get(alias) for alias in alias_li]
            for col in column_li:
                for table, table_cols in table_cols_mapping.items():
                    if col in table_cols and table not in table_li:
                        table_li.append(table)
        else:
            table_li = [table_alias_mapping.get(alias) for alias in alias_li]
        return table_li, table_cols_mapping

    async def _insert_history_record(self, table_name, cols, ids, current_time, delete=False):
        if not ids:
            return None
        history_table = table_name + self._history_posix
        base_columns = ['base_' + name for name in self.base_column]
        history_col = ','.join(cols + self.history_additional_cols + base_columns)
        main_table = table_name
        col = ','.join(cols)
        record_end_time = current_time if delete else self._record_end_time
        sql = f"""
            insert into {history_table} ({history_col}) 
            select {col}, '{current_time}', '{record_end_time}', '{self._record_operate_user}', 
            {','.join(self.base_column)} from {main_table} where id in ({ids})
        """
        await self._execute_history_dml(sql)

    async def _execute_update(self, stream, query, args, args_many=False):
        index = 0
        alias_li, column_li, alias_table_mapping, condition_sql_li, q_args = stream.extract_update_info(args, args_many)
        table_li, table_cols_mapping = await self._get_table_from_update_info(alias_li, column_li, alias_table_mapping)
        table_alias_mapping = {table: alias for alias, table in alias_table_mapping.items()}
        cols = ','.join([table_alias_mapping.get(table) + '.id' for table in table_li])
        query_pk_sql = "select " + cols + ''.join(condition_sql_li)
        pks = await self._query_record_pk(query_pk_sql, q_args, args_many, cols)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await self._end_history_record(table_li, pks, current_time)
        ret = (await self._origin_executemany(query, args)) if args_many else (await self._origin_execute(query, args))
        for table in table_li:
            table_cols = [name for name in table_cols_mapping.get(table) if name not in self.base_column]
            ids = [str(pk[index]) for pk in pks]
            await self._insert_history_record(table, table_cols, ','.join(ids), current_time)
            index += 1
        return ret

    async def _end_history_record(self, table_li, pks, current_time):
        index = 0
        cursor = await self._get_history_cursor()
        for table_name in table_li:
            base_ids = ','.join([str(pk[index]) for pk in pks])
            if not base_ids:
                continue
            sql = f"""
                    update {table_name + self._history_posix} set record_end_time = '{current_time}' 
                    where base_id in ({base_ids}) and record_end_time = '{self._record_end_time}'
                    """
            await cursor.execute(sql, None)
            index += 1
        await cursor.close()

    async def _execute_delete(self, stream, query, args, args_many=False):
        index = 0
        table_name_li, condition_sql, table_alias_mapping = stream.extract_delete_info()
        cols = ','.join([table + '.id' for table in table_name_li])
        query_pk_sql = "select " + cols + ''.join(condition_sql)
        pks = await self._query_record_pk(query_pk_sql, args, args_many, cols)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_li = [table_alias_mapping.get(table) for table in table_name_li]
        table_cols_mapping = {table: await self._extract_table_column(table) for table in table_alias_mapping.values()}
        await self._end_history_record(table_li, pks, current_time)
        for table in table_li:
            table_cols = [name for name in table_cols_mapping.get(table) if name not in self.base_column]
            ids = [str(pk[index]) for pk in pks]
            await self._insert_history_record(table, table_cols, ','.join(ids), current_time, delete=True)
            index += 1
        ret = (await self._origin_executemany(query, args)) if args_many else (await self._origin_execute(query, args))
        return ret

    async def execute(self, query, args=None, history_operate=None, operate_user=None):
        """
        Execute a query
        :param query: Query to execute.
        :param args: parameters used with query. (optional)
        :param history_operate: whether to operate history table. (optional)
        :param operate_user: operate history table user. (optional)
        :return: Number of affected rows
        :rtype: int
        If args is a list or tuple, %s can be used as a placeholder in the query.
        If args is a dict, %(name)s can be used as a placeholder in the query.
        """
        if history_operate is False:
            return await self._origin_execute(query, args)
        if self.history_operate is False and not history_operate:
            return await self._origin_execute(query, args)
        if operate_user:
            assert isinstance(operate_user, str), "operate user field must be a string."
            self._record_operate_user = operate_user
        stream = ParseSQL(query, self.base_column)
        query_type = stream.get_stmt_type()
        if query_type == DMLType.DELETE.value:
            return await self._execute_delete(stream, query, args)
        elif query_type == DMLType.UPDATE.value:
            return await self._execute_update(stream, query, args)
        elif query_type == DMLType.INSERT.value:
            return await self._process_insert(stream, query, args)
        else:
            return await self._origin_execute(query, args)

    async def executemany(self, query, args, history_operate=None, operate_user=None):
        # type: (str, list) -> int
        """Run several data against one query

        :param query: query to execute on server
        :param args:  Sequence of sequences or mappings.  It is used as parameter.
        :param history_operate: whether to operate history table. (optional)
        :param operate_user: operate history table user. (optional)
        :return: Number of rows affected, if any.

        This method improves performance on multiple-row INSERT and
        REPLACE. Otherwise it is equivalent to looping over args with
        execute().
        """
        if history_operate is False:
            return await self._origin_executemany(query, args)
        if self.history_operate is False and not history_operate:
            return await self._origin_executemany(query, args)
        if operate_user:
            assert isinstance(operate_user, str), "operate user field must be a string."
            self._record_operate_user = operate_user
        stream = ParseSQL(query, self.base_column)
        query_type = stream.get_stmt_type()
        if query_type == DMLType.DELETE.value:
            return await self._execute_delete(stream, query, args, True)
        elif query_type == DMLType.UPDATE.value:
            return await self._execute_update(stream, query, args, True)
        elif query_type == DMLType.INSERT.value:
            return await self._process_insert(stream, query, args, True)
        else:
            return await self._origin_executemany(query, args)

    async def _origin_execute(self, query, args=None):
        """Executes the given operation

                Executes the given operation substituting any markers with
                the given parameters.

                For example, getting all rows where id is 5:
                  cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))

                :param query: ``str`` sql statement
                :param args: ``tuple`` or ``list`` of arguments for sql query
                :returns: ``int``, number of rows that has been produced of affected
                """
        conn = self._get_db()
        while (await self.nextset()):
            pass
        if args is not None:
            query = query % self._escape_args(args, conn)
        await self._query(query)
        self._executed = query
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    async def _origin_executemany(self, query, args):
        """Execute the given operation multiple times

                The executemany() method will execute the operation iterating
                over the list of parameters in seq_params.

                Example: Inserting 3 new employees and their phone number

                    data = [
                        ('Jane','555-001'),
                        ('Joe', '555-001'),
                        ('John', '555-003')
                        ]
                    stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s')"
                    await cursor.executemany(stmt, data)

                INSERT or REPLACE statements are optimized by batching the data,
                that is using the MySQL multiple rows syntax.

                :param query: `str`, sql statement
                :param args: ``tuple`` or ``list`` of arguments for sql query
                """
        if not args:
            return

        if self._echo:
            logger.info("CALL %s", query)
            logger.info("%r", args)

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1)
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            return (await self._do_execute_many(
                q_prefix, q_values, q_postfix, args, self.max_stmt_length,
                self._get_db().encoding))
        else:
            rows = 0
            for arg in args:
                await self.execute(query, arg)
                rows += self._rowcount
            self._rowcount = rows
        return self._rowcount

    async def _do_execute_many(self, prefix, values, postfix, args,
                               max_stmt_length, encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, str):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, str):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, str):
            v = v.encode(encoding, 'surrogateescape')
        sql += v
        rows = 0
        pairs = list()
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, str):
                v = v.encode(encoding, 'surrogateescape')
            if len(sql) + len(v) + len(postfix) + 1 > max_stmt_length:
                last_rowid, r = await self._origin_execute_pairs(sql + postfix)
                rows += r
                pairs.append((last_rowid, r))
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        last_rowid, r = await self._origin_execute_pairs(sql + postfix)
        rows += r
        pairs.append((last_rowid, r))
        self.pairs = pairs
        self._rowcount = rows
        return rows

    async def _origin_execute_pairs(self, query, args=None):
        """Execute a query

        :param str query: Query to execute.

        :param args: parameters used with query. (optional)
        :type args: tuple, list or dict

        :return: a pairs about (first rowid and Number of affected rows)
        :rtype: int

        If args is a list or tuple, %s can be used as a placeholder in the query.
        If args is a dict, %(name)s can be used as a placeholder in the query.
        """
        conn = self._get_db()
        while (await self.nextset()):
            pass
        if args is not None:
            query = query % self._escape_args(args, conn)
        await self._query(query)
        self._executed = query
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._lastrowid, self._rowcount

    async def execute_history(self, query, args=None, history_time=None):
        """
        Query for a history list of results from history table.
        """
        if self.history_operate is False:
            return await self._origin_execute(query, args)
        stream = ParseSQL(query, self.base_column)
        history_query = stream.history_query(history_time, self._history_posix)
        return await self._origin_execute(history_query, args)

    async def supply_history_data(self, table_name, ids=None, operate_user=None):
        """
        补充历史数据
        使用场景：用于主表已有数据下后续开启历史拉链表时，补充主表已有数据但在历史拉链表里不存在的数据
        :params table_name: 主表名称
        :params ids: 选填参数 传入ids时，使用传入的id,用于补充指定数据, 未传入时, 补充整表数据
        :params operate_user: 选填参数 历史操作人
        """
        if operate_user:
            assert isinstance(operate_user, str), "operate user field must be a string."
            self._record_operate_user = operate_user
        dict_cursor = False
        if isinstance(self, (DictCursor, SSDictCursor)):
            dict_cursor = True
        if ids is None:
            # 获取整表数据的id
            query_sql = "SELECT id FROM {}".format(table_name)
            await self.execute(query_sql, [])
            query_data = self.fetchall()
            ids = [i["id"] for i in query_data] if dict_cursor else [i[0] for i in query_data]
            if not ids:
                return
        else:
            if not ids or not isinstance(ids, list):
                raise ValueError("传入id不能为空")
            id_str = ",".join([str(i) for i in ids])
            check_sql = "SELECT id FROM {} WHERE id IN ({})".format(table_name, id_str)
            await self.execute(check_sql, [])
            check_data = self.fetchall()
            ids = [i["id"] for i in check_data] if dict_cursor else [i[0] for i in check_data]

        id_str = ','.join([str(i) for i in ids])
        history_query_sql = "SELECT base_id FROM {}{} WHERE base_id IN ({})".format(table_name,
                                                                                    self._history_posix, id_str)
        await self.execute(history_query_sql, [])
        history_data = self.fetchall()
        base_id_list = [i["base_id"] for i in history_data] if dict_cursor else [i[0] for i in history_data]

        insert_id_list = list(set(ids) - set(base_id_list))
        if insert_id_list:
            # 待补充的id
            id_str = ",".join([str(i) for i in insert_id_list])
            col_name = [name for name in await self._extract_table_column(table_name) if name not in self.base_column]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            await self._insert_history_record(table_name, col_name, id_str, current_time)
        return insert_id_list, base_id_list

    async def analysis_process(self, main_table, base_id):
        """
        解析数据变更过程
        """
        history_data, col_name = await self._query_history_data(main_table, base_id)
        if not history_data:
            raise ValueError("回滚数据不存在")
        analysis_result = list()
        for idx, item in enumerate(history_data):
            previous_data = {} if idx == 0 else history_data[idx - 1]
            if idx == 0:
                analysis_result.append({"data_type": "insert", "data": item, "change": {}})
            elif item["record_begin_time"] == item["record_end_time"]:
                analysis_result.append({"data_type": "delete", "data": item, "change": {}})
            else:
                change_data = self.compare_difference(item, previous_data, col_name)
                analysis_result.append({"data_type": "update", "data": item, "change": change_data})
        return analysis_result

    async def rollback(self, main_table, history_data_id):
        """
        恢复数据到历史数据的哪一条
        """
        # 主表变更的字段
        col_name = [name for name in await self._extract_table_column(main_table) if name not in self.base_column]
        history_table = main_table + self._history_posix
        base_columns = ['base_' + name for name in self.base_column]
        history_col = ','.join(col_name + self.history_additional_cols + base_columns)
        data_sql = f"SELECT {history_col} FROM {history_table} WHERE id = %s"
        ret = await self.execute(data_sql, [history_data_id])
        history_data = await self.fetchone()
        if not history_data:
            raise ValueError("历史数据不存在")
        last_data = await self._query_history_last_data(history_table, history_data["base_id"], history_col)
        if last_data["id"] == int(history_data_id):
            # 表示待回滚数据是主表中的最新数据, 不需要回滚
            raise ValueError("待回滚数据为主表中的最新数据, 不需要回滚")
        # 判断回滚的数据在主表中的当前状态，是否已删除，删除则在主表数据插入一条，否则更新主表数据到当前指定的位置
        col = ",".join(col_name)
        if last_data["record_begin_time"] == last_data["record_end_time"]:
            # 数据已删除，则新增一条数据
            col_val = ",".join(["%({})s".format(c) for c in col_name])
            data_sql = f"INSERT INTO {main_table}({col}) VALUES({col_val})"
        else:
            col_val = ",".join(["{c}=%({c})s".format(c=c) for c in col_name])
            data_sql = f"UPDATE {main_table} SET {col_val} WHERE id = %(base_id)s"
        return data_sql, history_data

    async def _query_history_last_data(self, history_table, base_id, history_col):
        """
        获取数据的状态，是否已被删除  query
        """
        data_sql = f"SELECT id, {history_col} FROM {history_table} WHERE base_id = %s ORDER BY id DESC limit 1"
        ret = await self.execute(data_sql, [base_id])
        return await self.fetchone()

    async def _query_history_data(self, main_table, base_id):
        """
        根据主表名查看历史数据
        """
        col_name = [name for name in await self._extract_table_column(main_table) if name not in self.base_column]
        history_table = main_table + self._history_posix
        base_columns = ['base_' + name for name in self.base_column]
        history_col = ','.join(col_name + self.history_additional_cols + base_columns)
        data_sql = f"SELECT {history_col} FROM {history_table} WHERE base_id = %s"
        ret = await self.execute(data_sql, [base_id])
        return await self.fetchall(), col_name

    @staticmethod
    def compare_difference(current_data, previous_data, col_name=None):
        """
        比对当前数据与上一条数据的差异，返回差异数据
        """
        difference = dict()
        for key, val in current_data.items():
            if col_name is None:
                if previous_data[key] != val:
                    difference[key] = {"current": val, "original": previous_data[key]}
            else:
                if key in col_name and previous_data[key] != val:
                    difference[key] = {"current": val, "original": previous_data[key]}
        return difference


class DeserializationCursor(_DeserializationCursorMixin, Cursor):
    """A cursor automatic deserialization of json type fields"""


class DictCursor(_DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


class SSCursor(Cursor):
    """Unbuffered Cursor, mainly useful for queries that return a lot of
    data, or for connections to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    async def close(self):
        conn = self._connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            await self._result._finish_unbuffered_query()

        try:
            while (await self.nextset()):
                pass
        finally:
            self._connection = None

    async def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        await conn.query(q, unbuffered=True)
        await self._do_get_result()
        return self._rowcount

    async def _read_next(self):
        """Read next row """
        row = await self._result._read_rowdata_packet_unbuffered()
        row = self._conv_row(row)
        return row

    async def fetchone(self):
        """ Fetch next row """
        self._check_executed()
        row = await self._read_next()
        if row is None:
            return
        self._rownumber += 1
        return row

    async def fetchall(self):
        """Fetch all, as per MySQLdb. Pretty useless for large queries, as
        it is buffered.
        """
        rows = []
        while True:
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    async def fetchmany(self, size=None):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.

        The number of rows returned can be specified using the size argument,
        which defaults to one

        :param size: ``int`` number of rows to return
        :returns: ``list`` of fetched rows
        """
        self._check_executed()
        if size is None:
            size = self._arraysize

        rows = []
        for i in range(size):
            row = await self._read_next()
            if row is None:
                break
            rows.append(row)
            self._rownumber += 1
        return rows

    async def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position
        according to mode . Same as :meth:`Cursor.scroll`, but move cursor
        on server side one by one row. If you want to move 20 rows forward
        scroll will make 20 queries to move cursor. Currently only forward
        scrolling is supported.

        :param int value: move cursor to next position according to mode.
        :param str mode: scroll mode, possible modes: `relative` and `absolute`
        """

        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise NotSupportedError("Backwards scrolling not supported "
                                        "by this cursor")

            for _ in range(value):
                await self._read_next()
            self._rownumber += value
        elif mode == 'absolute':
            if value < self._rownumber:
                raise NotSupportedError(
                    "Backwards scrolling not supported by this cursor")

            end = value - self._rownumber
            for _ in range(end):
                await self._read_next()
            self._rownumber = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)


class SSDictCursor(_DictCursorMixin, SSCursor):
    """An unbuffered cursor, which returns results as a dictionary """


def connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None, operate_history=False, postfix='_history',
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            no_delay=None, autocommit=False, echo=False,
            local_infile=False, loop=None, ssl=None, auth_plugin='',
            program_name='', server_public_key=None, base_column=None):
    """See connections.Connection.__init__() for information about
    defaults."""
    coro = _connect(host=host, user=user, password=password, db=db,
                    port=port, unix_socket=unix_socket, charset=charset,
                    sql_mode=sql_mode, read_default_file=read_default_file,
                    conv=conv, use_unicode=use_unicode,
                    client_flag=client_flag, cursorclass=cursorclass,
                    init_command=init_command, operate_history=operate_history,
                    connect_timeout=connect_timeout, postfix=postfix,
                    read_default_group=read_default_group, server_public_key=server_public_key,
                    no_delay=no_delay, autocommit=autocommit, echo=echo,
                    local_infile=local_infile, loop=loop, ssl=ssl,
                    auth_plugin=auth_plugin, program_name=program_name, base_column=base_column)
    return _ConnectionContextManager(coro)


async def _connect(*args, **kwargs):
    conn = Connection(*args, **kwargs)
    await conn._connect()
    return conn
