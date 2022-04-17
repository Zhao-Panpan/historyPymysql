#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

    pymysql 操作方法

"""

from DBUtils.PooledDB import PooledDB

from . import pymysql_connection


class GenConnection(object):
    """
    数据库连接池
    """
    _pool = None

    def __init__(self):
        if GenConnection._pool is None:
            self.init_pool()
        self.conn = self._pool.connection()

    @staticmethod
    def init_pool():
        db_conf = settings.DATABASES.get('default')
        mysql_param = {
            'host': db_conf.get('HOST'),
            'user': db_conf.get('USER'),
            'passwd': db_conf.get('PASSWORD'),
            'db': db_conf.get('NAME'),
            'port': db_conf.get('PORT'),
            'charset': 'utf8',
            'base_column': settings.BASE_COLUMN,
            'sql_mode': settings.SQL_MODE,
            'connect_timeout': 5,
            'mincached': 1,
            'maxcached': 20,
            'maxconnections': 20,
            'blocking': True,
        }
        GenConnection._pool = PooledDB(creator=pymysql_connection, **mysql_param)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
            self.conn.close()


def operate_db(statements, history_operate=True, operate_user=None):
    """
    数据库的增删改操作；
    :param statements: 要执行的请求体
    :param history_operate: 是否开启历史拉链表 默认开启
    :param operate_user: 操作人
    :return:
    """
    ret_ids = list()
    with GenConnection() as conn:
        with conn.cursor() as cur:
            for sql, param in statements:
                cur.execute(sql, param, history_operate=history_operate, operate_user=operate_user)
                ret_ids.append((cur.lastrowid, cur.rowcount))
    return ret_ids


def operate_db_many(statement, params, history_operate=True, operate_user=None):
    """
    数据库的增删改操作；
    :param statement: 要执行的sql语句
    :param params: 传入的参数
    :param history_operate: 是否开启历史操作，默认开启
    :param operate_user: 操作人
    :return cur.pairs:[(last,id,rowcount),(last_id,rowcount)], 语句可能会被拆成多天sql分别执行，pairs里面包括每条执行sql的last id 及 rowcount
    :return cur.rowcount: 总行数
    """
    with GenConnection() as conn:
        with conn.cursor() as cur:
            cur.executemany(statement, params, history_operate=history_operate, operate_user=operate_user)
            return cur.pairs, cur.rowcount


def query_db(statement, params, cursor_type=None):
    '''
    数据库的查询操作
    :param statement: 要执行的sql语句
    :param params: 要查询参数条件
    :param cursor_type:
    :return: 查询出来的结果：tuple
    '''
    cursor_type_map = {
        'Cursor': pymysql_connection.Cursor,
        'SSCursor': pymysql_connection.SSCursor,
        'DictCursor': pymysql_connection.DictCursor,
        'SSDictCursor': pymysql_connection.SSDictCursor
    }
    cursor = cursor_type_map[cursor_type] if cursor_type_map.get(cursor_type) else cursor_type_map["Cursor"]
    with GenConnection() as conn:
        with conn.cursor(cursor=cursor) as cur:
            cur.execute(statement, params)
            return cur.fetchall()


def initialize_db(cursor_type=None,  operate_user=None):
    """
    获取数据库的 conn cursor
    :param cursor_type: cursor类型 字符串值
    :param operate_user: 记录操作人 字符串值
    :return:
    """
    cursor_type_map = {
        'Cursor': pymysql_connection.Cursor,
        'SSCursor': pymysql_connection.SSCursor,
        'DictCursor': pymysql_connection.DictCursor,
        'SSDictCursor': pymysql_connection.SSDictCursor
    }
    cursor = cursor_type_map[cursor_type] if cursor_type_map.get(cursor_type) else cursor_type_map["Cursor"]
    conn = GenConnection().conn
    cur = conn.cursor(cursor=cursor, operate_user=operate_user)
    return conn, cur


def close_db(conn, cur):
    """
    关闭连接
    :return:
    """
    cur.close()
    conn.close()


def query_execute(statement, params, cur):
    """
    数据库 多条查询  返回一个列表 [{"a": 1, "b":1, ...}, ...]
    :param statement: 要执行的sql语句
    :param params: 要查询参数条件
    :param cur: 连接光标
    :return:
    """
    cur.execute(statement, params)
    return cur.fetchall()


def query_fetchone(statement, params, cur):
    """
    查询一条  返回一个 字典，查询数据不存在时，返回None
    :param statement: 要执行的sql语句
    :param params: 要查询参数条件
    :param cur: 连接光标
    :return:
    """
    cur.execute(statement, params)
    return cur.fetchone()


def operate(statement, params, conn, cur, commit=False, history_operate=True, operate_user=None):
    """
    操作单条数据
    :param statement: 要执行的sql语句
    :param params: 传入的参数
    :param conn: 数据库连接
    :param cur: 连接光标
    :param commit: 是否提交
    :param history_operate: 是否开启历史操作，默认开启
    :param operate_user: 操作人
    :return:
    """
    try:
        # 执行SQL语句
        cur.execute(statement, params, history_operate=history_operate, operate_user=operate_user)
        if commit:
            # 提交事务
            conn.commit()
    except Exception as e:
        # 有异常，回滚事务
        if commit:
            conn.rollback()
        raise ValueError(str(e))
    return cur.lastrowid, cur.rowcount


def batch_operate(statement, params, conn, cur, commit=False, history_operate=True, operate_user=None):
    """
    操作单条数据
    :param statement: 要执行的sql语句
    :param params: 传入的参数
    :param conn: 数据库连接
    :param cur: 连接光标
    :param commit: 是否提交
    :param history_operate: 是否开启历史操作，默认开启
    :param operate_user: 操作人
    :return:
    """
    try:
        # 执行SQL语句
        cur.executemany(statement, params, history_operate=history_operate, operate_user=operate_user)
        if commit:
            # 提交事务
            conn.commit()
    except Exception as e:
        # 有异常，回滚事务
        if commit:
            conn.rollback()
        raise ValueError(str(e))
    return cur.pairs, cur.rowcount


def supply_history_data(table_name, ids=None, operate_user=None):
    """
    补充历史数据
        使用场景：用于主表已有数据下后续开启历史拉链表时，补充主表已有数据但在历史拉链表里不存在的数据
        :params table_name: 主表名称
        :params ids: 选填参数 传入ids时，使用传入的id,用于补充指定数据, 未传入时, 补充整表数据
        :params operate_user: 选填参数 历史操作人
    """
    with GenConnection() as conn:
        with conn.cursor() as cur:
            ins_list, exist_list = cur.supply_history_data(table_name, ids=ids, operate_user=operate_user)
            print("表:{} 插入成功{}条, 成功数据id为{}, 已存在历史表数据{}条, 已存在数据id为{}".format(table_name, len(ins_list),
                                                                            "、".join([str(i) for i in ins_list]),
                                                                            len(exist_list),
                                                                            "、".join([str(i) for i in exist_list])))


def rollback_history_data(table_name, history_data_id, operate_user=None):
    """
    回滚历史数据
    :params table_name: 主表名称
    :params history_data_id: 历史记录数据的id  id为要恢复到某一条数据
    :params operate_user: 历史记录操作人
    """
    conn, cursor = initialize_db(cursor_type="DictCursor")
    try:
        data_sql, data_args = cursor.rollback(table_name, history_data_id)
        cursor.execute(data_sql, data_args, history_operate=True, operate_user=operate_user)
    except Exception as exp:
        conn.rollback()
        raise ValueError(exp)
    else:
        conn.commit()
        return cursor.rowcount
    finally:
        close_db(conn, cursor)


def history_change_process(table_name, data_id):
    """
    获取某条数据的历史变更过程
    """
    with GenConnection() as conn:
        with conn.cursor() as cur:
            return cur.analysis_process(table_name, data_id)


if __name__ == '__main__':
    pass
