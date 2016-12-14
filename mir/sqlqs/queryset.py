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


class Schema(namedtuple('Schema', 'name,columns,constraints,relation_class')):

    def __new__(cls, name, columns, constraints):
        relation_class = namedtuple(name, [column.name for column in columns])
        return super().__new__(cls, name, columns, constraints, relation_class)


ColumnDef = namedtuple('ColumnDef', 'name,constraints')


class PureQuerySet:

    def __init__(self, schema, source):
        self._schema = schema
        self._source = source

    def __str__(self):
        return self._select_query

    @property
    def _select_query(self):
        columns_string = ','.join(
            column.name for column in self._schema
        )
        return 'SELECT {columns} from {source}'.format(
            columns=columns_string,
            source=self._source,
        )


class QuerySet(collections.abc.Set, PureQuerySet):

    def __init__(self, conn, schema, source):
        super().__init__(schema, source)
        self._conn = conn

    def __iter__(self):
        cur = self._conn.cursor()
        cur.execute(self._select_query)

    def __contains__(self, relation):
        return relation in set(self)

    def __len__(self):
        return len(set(self))
