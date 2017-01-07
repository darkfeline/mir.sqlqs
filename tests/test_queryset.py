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

from unittest import mock

import pytest

import mir.sqlqs.queryset as queryset


def test_query_repr():
    query = queryset.Query('foo', ('bar',))
    assert repr(query) == "Query('foo', ('bar',))"


def test_query_bool_false():
    query = queryset.Query('', ())
    assert not query


def test_query_bool_true_sql():
    query = queryset.Query('foo', ())
    assert not query


def test_query_bool_true_params():
    query = queryset.Query('', ('foo',))
    assert not query


def test_query_add_query():
    query1 = queryset.Query('foo', ('foo',))
    query2 = queryset.Query('bar', ('bar',))
    got = query1 + query2
    assert got.sql == 'foobar'
    assert got.params == ('foo', 'bar')


def test_query_add_string():
    query = queryset.Query('foo', ('foo',))
    got = query + 'bar'
    assert got.sql == 'foobar'
    assert got.params == ('foo',)


def test_query_add_wrong_type():
    query = queryset.Query('foo', ('foo',))
    with pytest.raises(TypeError):
        query + 1


def test_query_and():
    query = queryset.Query('foo', ('foo',))
    got = query & 'bar'
    assert got.sql == 'foo AND bar'
    assert got.params == ('foo',)


def test_query_or():
    query = queryset.Query('foo', ('foo',))
    got = query | 'bar'
    assert got.sql == 'foo OR bar'
    assert got.params == ('foo',)


def test_query_get_query():
    query = queryset.Query('foo', ('foo',))
    assert query is query._get_query()


def test_schema_primary_key():
    got = queryset.Schema._find_primary_key(
        [
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
    )
    assert got == 'name'


def test_schema_multiple_primary_key():
    with pytest.raises(ValueError):
        queryset.Schema._find_primary_key(
            [
                queryset.Column(name='name', constraints=['PRIMARY KEY']),
                queryset.Column(name='subgroup', constraints=['PRIMARY KEY']),
            ]
        )


def test_schema_missing_primary_key():
    got = queryset.Schema._find_primary_key(
            [
                queryset.Column(name='name', constraints=[]),
                queryset.Column(name='subgroup', constraints=[]),
            ]
        )
    assert got == 'rowid'


def test_schema_execute(conn):
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    cur = conn.cursor()
    schema.execute_with(cur)
    cur.execute("SELECT name FROM sqlite_master"
                " WHERE type='table' AND name='members'")
    assert len(cur.fetchall()) == 1


def test_schema_str():
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    assert str(schema) == (
        'CREATE TABLE "members" ('
        '"name" PRIMARY KEY,'
        '"subgroup" NOT NULL'
        ')')


def test_schema_repr():
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    with mock.patch.object(queryset.Column, '__repr__', return_value='foo'):
        assert repr(schema) == "Schema('members', [foo, foo], [])"


def test_column_str():
    col = queryset.Column('dorks', ('not nene',))
    got = str(col)
    assert got == '"dorks" not nene'


def test_queryset_repr():
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=mock.sentinel.conn,
        schema=schema,
    )
    with mock.patch.object(queryset.Schema, '__repr__') as schema_repr:
        schema_repr.return_value = 'schema'
        assert repr(qs) == "QuerySet(sentinel.conn, schema, '')"


def test_queryset_iter(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        schema=schema,
    )

    assert set(qs) == {
        schema.row_class(name='maki', subgroup='bibi'),
        schema.row_class(name='umi', subgroup='lily white'),
    }


def test_queryset_filter(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        schema=schema,
        where_expr="subgroup='bibi'",
    )

    assert set(qs) == {
        schema.row_class(name='maki', subgroup='bibi'),
    }


def test_queryset_len(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        schema=schema,
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

    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        schema=schema,
    )

    assert schema.row_class(name='maki', subgroup='bibi') in qs


def test_queryset_not_contains(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'bibi'), ('umi', 'lily white')")

    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=conn,
        schema=schema,
    )

    assert schema.row_class(name='maki', subgroup='printemps') not in qs


def test_queryset_query():
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    qs = queryset.QuerySet(
        conn=mock.sentinel.dummy,
        schema=schema,
    )

    assert qs._get_query().sql == 'SELECT "name","subgroup" FROM "members"'


def test_table_add(conn):
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    table = queryset.Table(
        conn=conn,
        schema=schema,
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")

    table.add(schema.row_class('maki', 'bibi'))

    cur.execute("SELECT * FROM members WHERE name='maki'")
    assert len(list(cur)) == 1


def test_table_add_update(conn):
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    table = queryset.Table(
        conn=conn,
        schema=schema,
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES"
                " ('maki', 'printemps')")

    table.add(schema.row_class('maki', 'bibi'))

    cur.execute("SELECT * FROM members WHERE name='maki'")
    assert list(cur) == [('maki', 'bibi')]


def test_table_discard(conn):
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    table = queryset.Table(
        conn=conn,
        schema=schema,
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")
    cur.execute("INSERT INTO members (name, subgroup) VALUES ('maki', 'bibi')")

    table.discard(schema.row_class('maki', 'bibi'))

    cur.execute("SELECT * FROM members WHERE name='maki'")
    assert not list(cur)


def test_table_sql():
    schema = mock.create_autospec(queryset.Schema, instance=True)
    table = queryset.Table(
        conn=mock.sentinel.conn,
        schema=schema,
    )
    assert table._get_sql() == schema._get_sql()


def test_table_discard_missing(conn):
    schema = queryset.Schema(
        name='members',
        columns=[
            queryset.Column(name='name', constraints=['PRIMARY KEY']),
            queryset.Column(name='subgroup', constraints=['NOT NULL']),
        ],
        constraints=[],
    )
    table = queryset.Table(
        conn=conn,
        schema=schema,
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE members ("
                "name PRIMARY KEY,"
                "subgroup NOT NULL"
                ")")

    table.discard(schema.row_class('maki', 'bibi'))

    cur.execute("SELECT * FROM members WHERE name='maki'")
    assert not list(cur)


def test_escape_string():
    got = queryset._escape_string('hello')
    assert got == "'hello'"


def test_escape_string_with_quotes():
    got = queryset._escape_string("sanzen'in")
    assert got == "'sanzen''in'"


def test_escape_name():
    got = queryset._escape_name('hello')
    assert got == '"hello"'
