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

from mir.sqlqs import migration


def _dummy(conn):
    pass  # pragma: no cover


def test_repr():
    manager = migration.MigrationManager()
    assert repr(manager) == '<MigrationManager with migrations={} final_ver=0>'


def test_register():
    manager = migration.MigrationManager()
    manager.register(migration.Migration(0, 1, _dummy))
    assert manager._migrations == {0: migration.Migration(0, 1, _dummy)}


def test_register_disjoint():
    manager = migration.MigrationManager()
    with pytest.raises(ValueError):
        manager.register(migration.Migration(1, 2, _dummy))


def test_migration_decorator():
    manager = migration.MigrationManager()
    manager.migration(0, 1)(_dummy)
    assert manager._migrations == {0: migration.Migration(0, 1, _dummy)}


def test_should_migrate(conn):
    manager = migration.MigrationManager()
    manager.migration(0, 1)(_dummy)
    assert manager.should_migrate(conn)


def test_should_not_migrate(conn):
    manager = migration.MigrationManager()
    assert not manager.should_migrate(conn)


def test_migrate(conn):
    manager = migration.MigrationManager()
    func = mock.Mock([])
    manager.migration(0, 1)(func)
    manager.migrate(conn)

    func.assert_called_once_with(conn)
    version = conn.cursor().execute('PRAGMA user_version').fetchone()[0]
    assert version == 1


def test_migrate_with_missing_migration(conn):
    manager = migration.MigrationManager(1)
    func = mock.Mock([])
    manager.migration(1, 2)(func)
    with pytest.raises(migration.MigrationError):
        manager.migrate(conn)


def test_foreign_key_errors(conn):
    manager = migration.MigrationManager(0)
    func = mock.Mock([])
    manager.migration(0, 1)(func)
    helper_patch = mock.patch(
        'mir.sqlqs.pragma.PragmaHelper.check_foreign_keys',
        autospec=True,
        return_value=[mock.sentinel.error],
    )
    with pytest.raises(migration.MigrationError), helper_patch:
        manager.migrate(conn)


def test_invalid_migration():
    with pytest.raises(ValueError):
        migration.Migration(2, 1, _dummy)
