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

from unittest import mock

import pytest

import mir.sqlqs.queryset as queryset


def test_table_create(conn):
    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    table.create(conn)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master"
                " WHERE type='table' AND name='members'")
    assert len(cur.fetchall()) == 1


def test_table_str():
    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    assert str(table) == (
        'CREATE TABLE "members" ('
        '"name" PRIMARY KEY,'
        '"subgroup" NOT NULL'
        ')')


def test_queryset_iter(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        table=table,
    )

    assert set(qs) == {
        table.row_class(name='maki', subgroup='bibi'),
        table.row_class(name='umi', subgroup='lily white'),
    }


def test_queryset_filter(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        table=table,
        where_expr="subgroup='bibi'",
    )

    assert set(qs) == {
        table.row_class(name='maki', subgroup='bibi'),
    }


def test_queryset_len(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        table=table,
    )

    assert len(qs) == 2


def test_queryset_contains(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        table=table,
    )

    assert table.row_class(name='maki', subgroup='bibi') in qs


def test_queryset_not_contains(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        table=table,
    )

    assert table.row_class(name='maki', subgroup='printemps') not in qs


def test_queryset_query():
    table = queryset.Table(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=mock.sentinel.dummy,
        table=table,
    )

    assert qs._select_query.sql == 'SELECT "name","subgroup" FROM "members"'


def test_query_bool_false():
    query = queryset.Query('', ())
    assert not query


def test_query_bool_true_sql():
    query = queryset.Query('foo', ())
    assert not query


def test_query_bool_true_parameters():
    query = queryset.Query('', ('foo',))
    assert not query


def test_query_add_query():
    query1 = queryset.Query('foo', ('foo',))
    query2 = queryset.Query('bar', ('bar',))
    got = query1 + query2
    assert got.sql == 'foobar'
    assert got.parameters == ('foo', 'bar')


def test_query_add_string():
    query1 = queryset.Query('foo', ('foo',))
    got = query1 + 'bar'
    assert got.sql == 'foobar'
    assert got.parameters == ('foo',)


def test_query_add_int():
    query1 = queryset.Query('foo', ('foo',))
    with pytest.raises(TypeError):
        query1 + 1
