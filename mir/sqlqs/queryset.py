# Copyright (C) 2016 Allen Li
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

from collections import namedtuple
import collections.abc
import itertools


class Table(namedtuple('Table', 'name,columns,constraints,row_class')):

    """Table schema

    Fields:

    name -- table name string
    columns -- sequence of ColumnDef instances
    constraints -- sequence of constraint strings

    Automatically generated:

    row_class -- namedtuple for table rows
    """

    __slots__ = ()

    def __new__(cls, name, columns, constraints):
        row_class = namedtuple(name, [column.name for column in columns])
        return super().__new__(cls, name, columns, constraints, row_class)

    def __str__(self):
        return self._create_query

    def create(self, conn):
        """Create a table with this schema."""
        cur = conn.cursor()
        cur.execute(self._create_query)

    @property
    def column_names(self):
        return [column.name for column in self.columns]

    @property
    def _create_query(self):
        """Return the corresponding create query."""
        return 'CREATE TABLE "{name}" ({defs})'.format(
            name=self.name,
            defs=','.join(itertools.chain(
                (str(column) for column in self.columns),
                self.constraints,
            )),
        )


class Column(namedtuple('Column', 'name,constraints')):

    """Column definition

    Fields:

    name -- column name as a string
    constraints -- sequence of constraint strings
    """

    __slots__ = ()

    def __str__(self):
        return ' '.join(itertools.chain(('"%s"' % self.name,),
                                        self.constraints))


class QuerySet(collections.abc.Set,
               namedtuple('QuerySet', 'conn,table,where_expr')):

    """SQL queries represented as sets

    Fields:

    conn -- database connection object
    table -- Table instance
    where_expr -- WHERE expression
    """

    __slots__ = ()

    def __new__(cls, conn, table, where_expr=''):
        return super().__new__(cls, conn, table, where_expr)

    def __iter__(self):
        cur = self.conn.cursor()
        cur.execute(self._select_query)
        row_class = self.table.row_class
        for row in cur:
            yield row_class._make(row)

    def __contains__(self, row):
        return row in set(self)

    def __len__(self):
        return len(set(self))

    def __str__(self):
        return self._select_query

    @property
    def _select_query(self):
        """Return the select query this set represents."""
        columns_string = ','.join(
            '"%s"' % column for column in self.table.column_names
        )
        query_parts = ['SELECT {columns} FROM {source}'.format(
            columns=columns_string,
            source='"%s"' % self.table.name,
        )]
        if self.where_expr:
            query_parts.append('WHERE %s' % self.where_expr)
        return ' '.join(query_parts)
