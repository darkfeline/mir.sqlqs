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


class PragmaHelper:

    __slots__ = ('_conn',)

    def __init__(self, conn):
        self._conn = conn

    def __repr__(self):
        return '{cls}({this._conn!r})'.format(
            cls=type(self).__qualname__,
            this=self)

    def _execute(self, *args):
        return self._conn.cursor().execute(*args)

    @property
    def foreign_keys(self):
        """Enforce foreign key constraints."""
        return self._execute('PRAGMA foreign_keys').fetchone()[0]

    @foreign_keys.setter
    def foreign_keys(self, value):
        value = 1 if value else 0
        # Parameterization doesn't work with PRAGMA, so we have to use string
        # formatting.  This is safe from injections because it coerces to int.
        self._execute('PRAGMA foreign_keys=%d' % value)

    def check_foreign_keys(self):
        """Check foreign keys for errors."""
        yield from self._execute('PRAGMA foreign_key_check')

    @property
    def user_version(self) -> int:
        """Database user version."""
        return self._execute('PRAGMA user_version').fetchone()[0]

    @user_version.setter
    def user_version(self, version: int):
        """Set database user version."""
        # Parameterization doesn't work with PRAGMA, so we have to use string
        # formatting.  This is safe from injections because it coerces to int.
        self._execute('PRAGMA user_version=%d' % version)
