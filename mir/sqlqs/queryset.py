# Copyright (C) 2016, 2017 Allen Li
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Relational SQL QuerySets"""

import abc
from collections import namedtuple
import collections.abc
import itertools


class Executable(metaclass=abc.ABCMeta):

    """SQL executable object interface.

    Implementing classes must implement the _get_query() method, which
    returns the parametrized query that would be executed.
    """

    @abc.abstractmethod
    def _get_query(self) -> 'Query':
        """Get the associated query.

        The returned query can be any object with the attributes sql and
        params set like a Query object.
        """
        raise NotImplementedError

    def execute_with(self, cur):
        """Execute query with cursor."""
        query = self._get_query()
        return cur.execute(query.sql, query.params)


class Query(Executable):

    """Parametrized query.

    Attributes:

    sql -- SQL parametrized query string
    params -- Parameters
    """

    __slots__ = ('sql', 'params')

    def __init__(self, sql, params=()):
        self.sql = sql
        self.params = params

    def __repr__(self):
        cls = type(self).__qualname__
        return f'{cls}({self.sql!r}, {self.params!r})'

    def __bool__(self):
        return bool(self.sql) and bool(self.params)

    def __add__(self, other):
        if isinstance(other, type(self)):
            return type(self)(self.sql + other.sql,
                              self.params + other.params)
        elif isinstance(other, str):
            return self + type(self)(other)
        else:
            return NotImplemented

    def __and__(self, other):
        return self + ' AND ' + other

    def __or__(self, other):
        return self + ' OR ' + other

    def _get_query(self):
        return self


class SimpleQuery(Executable):

    """Interface for objects with a non-parametrized SQL representation.

    Abstract methods:

    _get_sql -- Get SQL to execute
    """

    def __str__(self):
        return self._get_sql()

    @abc.abstractmethod
    def _get_sql(self):
        raise NotImplementedError

    def _get_query(self):
        return Query(self._get_sql())


class Schema(SimpleQuery):

    """Table schema

    Attributes:

    name -- Table name
    primary_key -- Primary key column name
    row_class -- namedtuple class for table rows
    column_names -- Tuple of column names
    column_names_sql -- Column names formatted as SQL: '"foo", "bar"'

    Methods:

    make_row -- Make row tuple
    """

    def __init__(self, name, columns, constraints):
        self.name = name
        self._columns = columns
        self._constraints = constraints
        self.row_class = namedtuple(name, (column.name for column in columns))

    @staticmethod
    def _find_primary_key(columns):
        """Find the primary key column name."""
        primary_key_cols = [col for col in columns if col.primary_key]
        if len(primary_key_cols) > 1:
            raise ValueError('More than one primary key: {columns!r}')
        elif primary_key_cols:
            return primary_key_cols[0].name
        else:
            return 'rowid'

    def __repr__(self):
        cls = type(self).__qualname__
        return (f'{cls}({self.name!r}, {self._columns!r},'
                f' {self._constraints!r})')

    @property
    def primary_key(self):
        return self._find_primary_key(self._columns)

    @property
    def column_names(self):
        return tuple(column.name for column in self._columns)

    @property
    def column_names_sql(self):
        return ','.join(
            _escape_name(column) for column in self.column_names
        )

    @property
    def _column_defs(self):
        return ','.join(itertools.chain(
            (str(column) for column in self._columns),
            self._constraints,
        ))

    def make_row(self, iterable):
        """Make row tuple."""
        return self.row_class._make(iterable)

    def _get_sql(self):
        return f'CREATE TABLE {_escape_name(self.name)} ({self._column_defs})'


class Column:

    """Column definition

    Fields:

    name -- column name as a string
    constraints -- sequence of constraint strings

    Properties:

    primary_key
    """

    def __init__(self, name, constraints=()):
        self.name = name
        self.constraints = constraints

    def __str__(self):
        return ' '.join(itertools.chain((_escape_name(self.name),),
                                        self.constraints))

    @property
    def primary_key(self):
        return any('primary key' in constraint.lower()
                   for constraint in self.constraints)


class QuerySet(collections.abc.Set, Executable):

    """SQL query as a set"""

    def __init__(self, conn, schema, where_expr=''):
        self._conn = conn
        self._schema = schema
        self._where_expr = where_expr

    def __repr__(self):
        cls = type(self).__qualname__
        return (f'{cls}({self._conn!r}, {self._schema!r},'
                f' {self._where_expr!r})')

    def __iter__(self):
        cur = self._conn.cursor()
        self.execute_with(cur)
        make_row = self._schema.make_row
        yield from (make_row(row) for row in cur)

    def __contains__(self, row):
        return row in frozenset(self)

    def __len__(self):
        return len(frozenset(self))

    def _get_query(self):
        columns = self._schema.column_names_sql
        source = _escape_name(self._schema.name)
        query = Query(f'SELECT {columns} FROM {source}')
        if self._where_expr:
            query += ' WHERE '
            query += self._where_expr
        return query


class Table(collections.abc.MutableSet, QuerySet, SimpleQuery):

    """SQL table as a set."""

    def __init__(self, conn, schema):
        super().__init__(conn, schema)

    def _get_sql(self):
        return self._schema._get_sql()

    def add(self, row):
        """Upsert."""
        cur = self._conn.cursor()
        with self._conn:
            self._get_update_query(row).execute_with(cur)
            if self._conn.changes() == 0:
                self._get_insert_query(row).execute_with(cur)

    def discard(self, row):
        cur = self._conn.cursor()
        with self._conn:
            self._get_discard_query(row).execute_with(cur)

    def _get_update_query(self, row):
        query = Query(f'UPDATE {_escape_name(self._schema.name)} SET ')
        query += self._get_joined_cols(row)
        query += Query(
            f' WHERE {_escape_name(self._schema.primary_key)}=?',
            (getattr(row, self._schema.primary_key),),
        )
        return query

    def _get_insert_query(self, row):
        table = _escape_name(self._schema.name)
        cols = self._schema.column_names_sql
        vals = ','.join('?' for _ in row)
        sql = f'INSERT INTO {table} ({cols}) VALUES ({vals})'
        return Query(sql, row)

    def _get_discard_query(self, row):
        query = Query(f'DELETE FROM {_escape_name(self._schema.name)} WHERE ')
        query += self._get_anded_cols(row)
        return query

    def _get_joined_cols(self, row):
        sql = ','.join(f'{_escape_name(col)}=?'
                       for col in self._schema.column_names)
        return Query(sql, row)

    def _get_anded_cols(self, row):
        sql = ' AND '.join(f'{_escape_name(col)}=?'
                           for col in self._schema.column_names)
        return Query(sql, row)


def _escape_string(string):
    """Escape SQL string."""
    string = string.replace("'", "''")
    return f"'{string}'"


def _escape_name(string):
    """Escape SQL identifier."""
    return f'"{string}"'
