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

from mir.sqlqs import amigration


def _dummy(conn):
    pass  # pragma: no cover


def test_repr():
    manager = amigration.MigrationManager()
    assert repr(manager) == '<MigrationManager with migrations={} final_ver=0>'


def test_register():
    manager = amigration.MigrationManager()
    manager.register(amigration.Migration(0, 1, _dummy))
    assert manager._migrations == {0: amigration.Migration(0, 1, _dummy)}


def test_register_disjoint():
    manager = amigration.MigrationManager()
    with pytest.raises(ValueError):
        manager.register(amigration.Migration(1, 2, _dummy))


def test_migration_decorator():
    manager = amigration.MigrationManager()
    manager.migration(0, 1)(_dummy)
    assert manager._migrations == {0: amigration.Migration(0, 1, _dummy)}


def test_should_migrate(aconn):
    manager = amigration.MigrationManager()
    manager.migration(0, 1)(_dummy)
    assert manager.should_migrate(aconn)


def test_should_not_migrate(aconn):
    manager = amigration.MigrationManager()
    assert not manager.should_migrate(aconn)


def test_migrate(aconn):
    manager = amigration.MigrationManager()
    func = mock.Mock([])
    manager.migration(0, 1)(func)
    manager.migrate(aconn)

    func.assert_called_once_with(aconn)
    version = aconn.execute('PRAGMA user_version').fetchone()[0]
    assert version == 1


def test_migrate_with_missing_migration(aconn):
    manager = amigration.MigrationManager(1)
    func = mock.Mock([])
    manager.migration(1, 2)(func)
    with pytest.raises(amigration.MigrationError):
        manager.migrate(aconn)


def test_foreign_key_errors(aconn):
    manager = amigration.MigrationManager(0)
    func = mock.Mock([])
    manager.migration(0, 1)(func)
    helper_patch = mock.patch(
        'mir.sqlqs.apragma.check_foreign_keys',
        autospec=True,
        return_value=[mock.sentinel.error],
    )
    with pytest.raises(amigration.MigrationError), helper_patch:
        manager.migrate(aconn)


def test_invalid_migration():
    manager = amigration.MigrationManager(0)
    with pytest.raises(ValueError):
        manager.register(amigration.Migration(1, 0, _dummy))
