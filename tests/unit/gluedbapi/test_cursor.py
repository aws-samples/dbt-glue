import unittest
from unittest import mock

from dbt_common.exceptions import DbtInternalError

from dbt.adapters.glue.gluedbapi.cursor import GlueCursor


class TestGlueCursor(unittest.TestCase):
    def test_execute_on_running_cursor_raises_internal_error(self) -> None:
        cursor = GlueCursor(connection=mock.MagicMock())
        cursor._is_running = True
        with self.assertRaisesRegex(DbtInternalError, "CursorAlreadyRunning"):
            cursor.execute("select 1")
