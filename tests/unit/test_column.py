import unittest

from dbt.adapters.glue.column import GlueColumn


class TestGlueColumn(unittest.TestCase):
    def test_column_quote(self):
        test_column = GlueColumn("col_name", "INT")
        self.assertEqual(test_column.quoted, "`col_name`")