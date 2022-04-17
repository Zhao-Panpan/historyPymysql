#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

    公用文件，用于解析sql和定义sql相关常量

"""

import re
from enum import Enum
from copy import deepcopy
from sqlparse import parse, tokens
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Parenthesis, Token
from sqlparse.tokens import Keyword, DML

INSERT_OPTION = ['LOW_PRIORITY', 'DELAYED', 'HIGH_PRIORITY', 'IGNORE']
DELETE_OPTION = ['LOW_PRIORITY', 'QUICK', 'IGNORE']
UPDATE_OPTION = ['LOW_PRIORITY', 'IGNORE']


class DMLType(Enum):
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'


class DeleteType(Enum):
    """
    delete type:
        :type1 delete from table1 where id =1
        :type2 delete from t1, t2 using tag_1 t1 join tag_1_history t2 where t1.id = t2.base_id
        :type3 delete t1, t2 from tag_1 t1 join tag_1_history t2 where t1.id = t2.base_id
    """
    TYPE1 = 1
    TYPE2 = 2
    TYPE3 = 3


class ParseSQL(object):
    """
    parsing a sql statement then return some info
    """

    def __init__(self, sql, base_column):
        self.tokens = parse(sql.strip().strip(";"))[0].tokens
        self._base_column = base_column

    def get_stmt_type(self) -> str:
        """
        a statement can be one of four types, INSERT, UPDATE, DELETE and None
        :return: the type of statement
        """
        first_token = self.tokens[0]
        if not first_token.ttype == DML:
            return None
        if first_token.value.upper() not in (dml_type.value for dml_type in DMLType):
            return None
        return first_token.value.upper()

    def extract_insert_table(self) -> str:
        """
        parse a INSERT statement returns the table name it insert
        :return:
        """
        if self.get_stmt_type() != DMLType.INSERT.value:
            return None
        for token in self.tokens:
            if isinstance(token, Identifier) and token.value.upper() not in INSERT_OPTION:
                return token.value
            elif isinstance(token, Function):
                return token.get_name()
        return None

    def extract_delete_info(self) -> (list, list, dict):
        """
        extract condition sql list, table name list and alias table mapping from a DELETE statement
        :return:
        condition_sql: the clause which determine to be deleted rows
        table_name_li: to be deleted table names in delete clause
        alias_table_mapping: a mapping that key is alias and value is real table
        """
        if self.get_stmt_type() != DMLType.DELETE.value:
            return None, None, None
        delete_type = self._get_delete_type()
        if delete_type == DeleteType.TYPE1.value:
            return self._extract_type1_delete_info()
        elif delete_type == DeleteType.TYPE2.value:
            return self._extract_type2_delete_info()
        elif delete_type == DeleteType.TYPE3.value:
            return self._extract_type3_delete_info()
        else:
            return None, None, None

    def extract_update_info(self, args, args_many) -> (list, list, dict, list, int):
        """
        extract condition sql list, table name list and alias table mapping from a DELETE statement
        :return:
        alias_li: the list of alias
        column_li: the columns that will be updated
        alias_table_mapping: a mapping that key is alias and value is real table
        condition_sql_li: the clause which determine to be deleted rows
        :return:
        """
        q_args = deepcopy(args)
        condition_sql_li = [' ', 'from', ' ']
        alias_li, column_li, alias_table_mapping = [], [], None
        set_see, where_see = False, False
        assign_value = ''
        for token in self.tokens:
            if (set_see is False and isinstance(token, (IdentifierList, Identifier)) and
                    token.value not in UPDATE_OPTION):
                condition_sql_li.append(token.value)
                alias_table_mapping = self.format_update_table_reference(token.value)
                condition_sql_li.append(' ')
            elif set_see is False and token.ttype == Keyword and token.value.upper() == 'SET':
                set_see = True
            elif set_see and where_see is False and not isinstance(token, Where):
                assign_value += token.value
            elif set_see and where_see is False and isinstance(token, Where):
                alias_li, column_li = self.format_update_assignment_list(assign_value, q_args, args_many)
                condition_sql_li.append(token.value)
                where_see = True
            elif where_see:
                condition_sql_li.append(token.value)
        if where_see is False:
            alias_li, column_li = self.format_update_assignment_list(assign_value, q_args, args_many)
        return alias_li, column_li, alias_table_mapping, condition_sql_li, q_args

    def _get_delete_type(self):
        from_see = False
        for token in self.tokens:
            if (from_see is False and isinstance(token, (Identifier, IdentifierList)) and
                    token.value.upper() not in DELETE_OPTION):
                return DeleteType.TYPE3.value
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                from_see = True
            elif from_see and token.ttype is Keyword and token.value.upper() == 'USING':
                return DeleteType.TYPE2.value
            elif from_see and isinstance(token, Where):
                return DeleteType.TYPE1.value
        return None

    def _extract_type1_delete_info(self):
        condition_token_li = [' ']
        table_alias_mapping = {}
        table_li = []
        from_see = False
        table_see = False
        for token in self.tokens:
            if from_see:
                condition_token_li.append(token.value)
                if table_see is False and isinstance(token, Identifier):
                    table_see = True
                    table_li.append(token.value)
                    table_alias_mapping[token.value] = token.value
            elif from_see is False and token.ttype is Keyword and token.value.upper() == 'FROM':
                from_see = True
                condition_token_li.append(token.value)
        return table_li, condition_token_li, table_alias_mapping

    def _extract_type2_delete_info(self):
        condition_token_li = [' ', 'from', ' ']
        table_li = []
        table_alias_mapping = {}
        from_see = False
        using_see = False
        where_see = False
        for token in self.tokens:
            if using_see:
                condition_token_li.append(token.value)
                if where_see is False and isinstance(token, Identifier):
                    # table have a alias or not
                    value, *key = token.value.split(' ')
                    table_alias_mapping[key and key[0] or value] = value
                if where_see is False and isinstance(token, Where):
                    where_see = True
            elif from_see and using_see is False and isinstance(token, (Identifier, IdentifierList)):
                table_li += token.value.replace(' ', '').split(',')
            elif using_see is False and from_see is False and token.ttype is Keyword and token.value.upper() == 'FROM':
                from_see = True
            elif using_see is False and token.ttype is Keyword and token.value.upper() == 'USING':
                using_see = True
        return table_li, condition_token_li, table_alias_mapping

    def _extract_type3_delete_info(self) -> (list, list, dict):
        condition_token_li = [' ', 'from', '']
        table_li = []
        table_alias_mapping = {}
        from_see = False
        where_see = False
        for token in self.tokens:
            if from_see:
                condition_token_li.append(token.value)
                if where_see is False and isinstance(token, Identifier):
                    # table have a alias or not
                    value, *key = token.value.split(' ')
                    table_alias_mapping[key and key[0] or value] = value
                if where_see is False and isinstance(token, Where):
                    where_see = True
            elif (from_see is False and isinstance(token, (Identifier, IdentifierList)) and
                  token.value not in DELETE_OPTION):
                table_li += token.value.replace(' ', '').split(',')
            elif from_see is False and token.ttype is Keyword and token.value.upper() == 'FROM':
                from_see = True
        return table_li, condition_token_li, table_alias_mapping

    def format_update_table_reference(self, table_reference: str) -> dict:
        """
        parsing a table_reference returns a dict, which precedently  make alias  or  secondarily table name as key,  table name as value
        :param table_reference: the str is mysql's table_reference eg: 'table1 t1, table2 t2'
        :return:
        """
        alias_table_mapping = {}
        for table_alias in table_reference.split(','):
            table, *alias = table_alias.strip().split(' ')
            alias_table_mapping[alias and alias[0] or table] = table
        return alias_table_mapping

    def delete_args(self, value, q_args, args_many):
        key = value[2:-2]
        pop_key = key if key else 0
        if args_many:
            for args in q_args:
                args.pop(pop_key)
        else:
            q_args.pop(pop_key)

    def cal_place_hold(self, token_li, q_args, args_many):
        for token in token_li:
            if token.ttype == tokens.Name.Placeholder:
                self.delete_args(token.value, q_args, args_many)
            elif hasattr(token, 'tokens'):
                self.cal_place_hold(token.tokens, q_args, args_many)

    def format_update_assignment_list(self, assignment_list: str, q_args, args_many) -> (list, list):
        """
        parsing assignment_list return alias list and column list
        :param assignment_list: the str is mysql's assignment_list
        :param q_args：
        :param args_many:
        :return:
        """
        alias_li, column_li = [], set()
        for assignment in assignment_list.split(','):
            table_col, value = assignment.strip().split('=')
            token_li = parse(value)[0].tokens
            self.cal_place_hold(token_li, q_args, args_many)
            *table, col = table_col.strip().split('.')
            if table and table[0] not in alias_li:
                alias_li.append(table[0])
            else:
                column_li.add(col)
        return alias_li, column_li

    def parse_table(self, token: Token, postfix: str) -> dict:
        """
        parse table token
        :param token:
        :param postfix: history table's postfix
        :return: table alias
        """
        table_name, *alias = token.value.strip().split(' ')
        history_table = ('`' + table_name[1:-1] + postfix + '`') if '`' in table_name else (table_name + postfix)
        return {(alias[0] if alias else history_table): table_name}

    def parse_identifier(self, token: Token, postfix: str, history_time: str) -> (list, {}):
        """
        parse table identifier
        :param token:
        :param postfix:
        :param history_time:
        :return: statement token list and table alias
        """
        stmt_token_value, alias = [], {}
        table_token = token.tokens[0]
        if isinstance(table_token, Parenthesis):
            stmt_token_value = self.parse_history_query(table_token, postfix, history_time)
            for sub_token in token.tokens[1:]:
                stmt_token_value.append(sub_token.value)
        elif isinstance(table_token, Token):
            stmt_token_value.append(token.value)
            alias = self.parse_table(token, postfix)
        else:
            raise ValueError('unexpect sql statement: {}'.format(token.value))
        return stmt_token_value, alias

    def parse_identifier_list(self, token: Token, postfix: str, history_time: str) -> (list, dict):
        """
        parse table identifier list
        :param token:
        :param postfix:
        :param history_time:
        :return: statement token list and table alias
        """
        stmt_token_value, alias = [], {}
        for sub_token in token.tokens:
            if isinstance(sub_token, IdentifierList):
                sub_token_value, sub_alias = self.parse_identifier_list(sub_token, postfix, history_time)
                stmt_token_value += sub_token_value
                alias.update(sub_alias)
            elif isinstance(sub_token, Identifier):
                sub_token_value, sub_alias = self.parse_identifier(sub_token, postfix, history_time)
                stmt_token_value += sub_token_value
                alias.update(sub_alias)
            else:
                stmt_token_value.append(sub_token.value)
        return stmt_token_value, alias

    def parse_table_references_token(self, token: Token, postfix: str, history_time: str) -> (list, dict):
        """
        parse table references
        :param token:
        :param postfix:
        :param history_time:
        :return: statement token list and table alias
        """
        if isinstance(token, IdentifierList):
            stmt_token_value, alias = self.parse_identifier_list(token, postfix, history_time)
        elif isinstance(token, Identifier):
            stmt_token_value, alias = self.parse_identifier(token, postfix, history_time)
        else:
            raise ValueError('unexpect sql statement: {}'.format(token.value))
        return stmt_token_value, alias

    def parse_where_token(self, token: Token, postfix: str, history_time: str, table_alias: dict) -> list:
        """
        parse where clause
        :param token:
        :param postfix:
        :param history_time:
        :param table_alias
        :return: statement token list
        """
        stmt_token_value = [token.tokens[0].value, '']
        stmt_token_value += self.concat_history_time(table_alias, history_time, True)
        for sub_token in token.tokens[1:]:
            if isinstance(sub_token, Parenthesis):
                stmt_token_value += self.parse_history_query(sub_token, postfix, history_time)
            else:
                stmt_token_value.append(self.trans_condition(sub_token))
        return stmt_token_value

    def trans_condition(self, token):
        """
        替换 where 条件中 字段在 base_column 中的值
        :param token:
        :return:
        """
        if token.ttype is None and "=" in token.value:
            key, val = token.value.split("=", 1)
            if "." in key.strip():
                name, aim = key.strip().split(".")
                token_value = "{}.base_{} ={}".format(name, aim, val) if aim in self._base_column else "{}={}".format(
                    key, val)
            else:
                token_value = "base_{} ={}".format(key.strip(),
                                                   val) if key.strip() in self._base_column else "{}={}".format(key,
                                                                                                                val)
        else:
            token_value = token.value
        return token_value

    def concat_history_time(self, table_alias: dict, history_time: str, where_see: bool) -> list:
        """
        add history query condition
        :param table_alias:
        :param history_time:
        :param where_see:
        :return: history query token list
        """
        stmt_token_value = []
        for alias in table_alias.keys():
            stmt_token_value.append(
                f" {alias}.record_begin_time <= '{history_time}' and {alias}.record_end_time >= '{history_time}' ")
            stmt_token_value.append(' and ')
        return stmt_token_value if where_see else stmt_token_value[:-1]

    def replace_table(self, stmt_token_value: list, alias_dict: dict, postfix: str) -> list:
        """
        replace a copy of sql token in which the table name has been replaced with history table
        :param stmt_token_value:
        :param alias_dict:
        :param postfix:
        :return: history sql token
        """
        stmt_value = ''.join(stmt_token_value)
        for table in set(alias_dict.values()):
            des_table = ('`' + table[1:-1] + postfix + '`') if '`' in table else (table + postfix)
            des_table = ' ' + des_table
            rex = re.compile(f'(?<=[ ,]){table}(?=[. ,])')
            stmt_value = rex.sub(des_table, stmt_value)
        return [stmt_value]

    def parse_history_query(self, tokens: Token, postfix: str, history_time: str) -> list:
        """
        parse sql's tokens, generate history query
        :param tokens:
        :param postfix:
        :param history_time:
        :return: history query token list
        """
        union_token_value = ''
        table_alias, stmt_token_value, sub_token = {}, [], []
        from_see, join_see, where_see, union_see = False, False, False, False

        for token in tokens:
            if union_see is False and token.value.upper() in ['UNION', 'UNION ALL']:
                union_see = True
                union_token_value = token.value
            elif union_see is True:
                sub_token.append(token)
            elif (from_see or join_see) and where_see is False and isinstance(token, (Identifier, IdentifierList)):
                token_value_li, alias = self.parse_table_references_token(token, postfix, history_time)
                stmt_token_value += token_value_li
                table_alias.update(alias)
            elif isinstance(token, Where):
                stmt_token_value += self.parse_where_token(token, postfix, history_time, table_alias)
                where_see = True
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                stmt_token_value.append(token.value)
                from_see = True
            elif token.ttype is Keyword and 'JOIN' in token.value.upper():
                stmt_token_value.append(token.value)
                join_see = True
            elif (where_see is False and token.ttype is Keyword
                  and token.value.upper() not in ['FROM', 'JOIN', 'ON', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN',
                                                  'FULL JOIN']):
                stmt_token_value += ['where ']
                stmt_token_value += self.concat_history_time(table_alias, history_time, False)
                where_see = True
                stmt_token_value.append(token.value)
            else:
                stmt_token_value.append(token.value)

        if where_see is False:
            stmt_token_value += [' ', 'where ']
            stmt_token_value += self.concat_history_time(table_alias, history_time, False)
        stmt_token_value = self.replace_table(stmt_token_value, table_alias, postfix)
        if sub_token:
            stmt_token_value += ['', union_token_value, ' ']
            stmt_token_value += self.parse_history_query(sub_token, postfix, history_time)
        return stmt_token_value

    def history_query(self, history_time: str, postfix: str = "_history") -> str:
        """
        gen history query from sql
        :param postfix:
        :param history_time:
        :return: history query
        """
        return ''.join(self.parse_history_query(self.tokens, postfix, history_time))
