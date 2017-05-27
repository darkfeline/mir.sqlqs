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

"""Simple database migrations (SQLAlchemy)"""

import logging
from typing import NamedTuple

from mir.sqlqs import apragma

logger = logging.getLogger(__name__)


class MigrationManager:

    """Simple database migration manager"""

    def __init__(self, initial_ver=0):
        self._migrations = {}
        self._final_ver = initial_ver

    def __repr__(self):
        cls = type(self).__qualname__
        return (f'<{cls} with migrations={self._migrations!r}'
                f' final_ver={self._final_ver!r}>')

    def register(self, migration: 'Migration'):
        """Register a migration.

        You can only register migrations in order.  For example, you can
        register migrations from version 1 to 2, then 2 to 3, then 3 to
        4.  You cannot register 1 to 2 followed by 3 to 4.
        """
        if migration.from_ver >= migration.to_ver:
            raise ValueError('Migration cannot downgrade verson')
        if migration.from_ver != self._final_ver:
            raise ValueError('Cannot register disjoint migration')
        self._migrations[migration.from_ver] = migration
        self._final_ver = migration.to_ver

    def migration(self, from_ver: int, to_ver: int):
        """Decorator to create and register a migration.

        >>> manager = MigrationManager()
        >>> @manager.migration(0, 1)
        ... def migrate(conn):
        ...     pass
        """
        def decorator(func):
            migration = Migration(from_ver, to_ver, func)
            self.register(migration)
            return func
        return decorator

    def migrate(self, conn):
        """Migrate a database as needed.

        This method is safe to call on an up-to-date database, on an old
        database, or an uninitialized database (version 0).

        This method is idempotent.
        """
        while self.should_migrate(conn):
            self._migrate_single(conn)

    def should_migrate(self, conn) -> bool:
        """Check if database needs migration."""
        return apragma.get_user_version(conn) < self._final_ver

    def _migrate_single(self, conn):
        """Perform a single migration starting from given version."""
        self._before_migration(conn)
        with conn.begin() as trans:
            migration = self._get_migration(apragma.get_user_version(conn))
            assert migration.from_ver == apragma.get_user_version(conn)
            logger.info('Migrating database from %d to %d',
                        migration.from_ver, migration.to_ver)
            migration.func(conn)
            self._check_database(conn)
            apragma.set_user_version(conn, migration.to_ver)
        self._after_migration(conn)

    def _get_migration(self, version):
        try:
            return self._migrations[version]
        except KeyError:
            raise MigrationError(
                f'No registered migration for version {version}')

    def _check_database(self, conn):
        foreign_key_errors = list(apragma.check_foreign_keys(conn))
        if foreign_key_errors:
            raise MigrationError(
                f'Foreign key check failed: {foreign_key_errors}')

    def _before_migration(self, conn):
        apragma.set_foreign_keys(conn, 0)

    def _after_migration(self, conn):
        apragma.set_foreign_keys(conn, 1)


class Migration(NamedTuple):
    from_ver: int
    to_ver: int
    func: 'Callable[[Connection], Any]'


class MigrationError(Exception):
    pass
