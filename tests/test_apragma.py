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

from mir.sqlqs import apragma


def test_get_foreign_keys(aconn):
    aconn.execute('PRAGMA foreign_keys=0')
    assert apragma.get_foreign_keys(aconn) == 0


def test_set_foreign_keys(aconn):
    apragma.set_foreign_keys(aconn, 1)
    got = aconn.execute('PRAGMA foreign_keys').fetchone()[0]
    assert got == 1


def test_get_user_version(aconn):
    aconn.execute('PRAGMA user_version=13')
    assert apragma.get_user_version(aconn) == 13


def test_set_user_version(aconn):
    apragma.set_user_version(aconn, 13)
    got = aconn.execute('PRAGMA user_version').fetchone()[0]
    assert got == 13


def test_check_foreign_keys(aconn):
    assert list(apragma.check_foreign_keys(aconn)) == []
