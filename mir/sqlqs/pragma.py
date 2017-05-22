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

"""SQLite PRAGMA helpers"""

import warnings


def get_foreign_keys(conn):
    """Return foreign_keys value."""
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys')
    return cur.fetchone()[0]


def set_foreign_keys(conn, value: int):
    """Set foreign_keys.

    Pass 1 for value to enable, 0 to disable.
    """
    cur = conn.cursor()
    # Parameterization doesn't work with PRAGMA, so we have to use string
    # formatting.
    cur.execute(f'PRAGMA foreign_keys={value}')


def check_foreign_keys(conn):
    """Check for foreign key errors.

    Return an iterator of rows with errors.
    """
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_key_check')
    yield from cur


def get_user_version(conn):
    """Return user_version value."""
    cur = conn.cursor()
    cur.execute('PRAGMA user_version')
    return cur.fetchone()[0]


def set_user_version(conn, value: int):
    """Set user_version."""
    cur = conn.cursor()
    # Parameterization doesn't work with PRAGMA, so we have to use string
    # formatting.
    cur.execute(f'PRAGMA user_version={value}')


class PragmaHelper:

    __slots__ = ('_conn',)

    def __init__(self, conn):
        warnings.warn('PragmaHelper is deprecated')
        self._conn = conn

    def __repr__(self):
        cls = type(self).__qualname__
        return f'{cls}({self._conn!r})'

    def _execute(self, *args):
        return self._conn.cursor().execute(*args)

    @property
    def foreign_keys(self):
        """Enforce foreign key constraints."""
        return get_foreign_keys(self._conn)

    @foreign_keys.setter
    def foreign_keys(self, value):
        value = 1 if value else 0
        set_foreign_keys(self._conn, value)

    def check_foreign_keys(self):
        """Check foreign keys for errors."""
        yield from check_foreign_keys(self._conn)

    @property
    def user_version(self) -> int:
        """Database user version."""
        return get_user_version(self._conn)

    @user_version.setter
    def user_version(self, version: int):
        """Set database user version."""
        set_user_version(self._conn, int(version))
