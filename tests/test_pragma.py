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

from mir.sqlqs.pragma import PragmaHelper


def test_set_foreign_keys(conn):
    helper = PragmaHelper(conn)
    helper.foreign_keys = True
    got = conn.cursor().execute('PRAGMA foreign_keys').fetchone()[0]
    assert got == 1


def test_get_foreign_keys(conn):
    conn.cursor().execute('PRAGMA foreign_keys=0')
    helper = PragmaHelper(conn)
    assert helper.foreign_keys == 0


def test_set_user_version(conn):
    helper = PragmaHelper(conn)
    helper.user_version = 13
    got = conn.cursor().execute('PRAGMA user_version').fetchone()[0]
    assert got == 13


def test_get_user_version(conn):
    conn.cursor().execute('PRAGMA user_version=13')
    helper = PragmaHelper(conn)
    assert helper.user_version == 13


def test_check_foreign_keys(conn):
    helper = PragmaHelper(conn)
    assert list(helper.check_foreign_keys()) == []


def test_pragma_helper_repr():
    helper = PragmaHelper(mock.sentinel.conn)
    assert repr(helper) == 'PragmaHelper(sentinel.conn)'
