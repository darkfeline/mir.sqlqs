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

from mir.sqlqs import pragma


def test_get_foreign_keys(conn):
    conn.cursor().execute('PRAGMA foreign_keys=0')
    assert pragma.get_foreign_keys(conn) == 0


def test_set_foreign_keys(conn):
    pragma.set_foreign_keys(conn, 1)
    got = conn.cursor().execute('PRAGMA foreign_keys').fetchone()[0]
    assert got == 1


def test_get_user_version(conn):
    conn.cursor().execute('PRAGMA user_version=13')
    assert pragma.get_user_version(conn) == 13


def test_set_user_version(conn):
    pragma.set_user_version(conn, 13)
    got = conn.cursor().execute('PRAGMA user_version').fetchone()[0]
    assert got == 13


def test_check_foreign_keys(conn):
    assert list(pragma.check_foreign_keys(conn)) == []
