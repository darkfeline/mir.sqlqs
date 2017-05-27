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

"""SQLite PRAGMA helpers (SQLAlchemy)."""


def get_foreign_keys(conn):
    """Return foreign_keys value."""
    result = conn.execute('PRAGMA foreign_keys')
    return result.fetchone()[0]


def set_foreign_keys(conn, value: int):
    """Set foreign_keys.

    Pass 1 for value to enable, 0 to disable.
    """
    # Parameterization doesn't work with PRAGMA, so we have to use string
    # formatting.
    conn.execute(f'PRAGMA foreign_keys={value}')


def check_foreign_keys(conn):
    """Check for foreign key errors.

    Return an iterator of rows with errors.
    """
    result = conn.execute('PRAGMA foreign_key_check')
    yield from result


def get_user_version(conn):
    """Return user_version value."""
    result = conn.execute('PRAGMA user_version')
    return result.fetchone()[0]


def set_user_version(conn, value: int):
    """Set user_version."""
    # Parameterization doesn't work with PRAGMA, so we have to use string
    # formatting.
    conn.execute(f'PRAGMA user_version={value}')
