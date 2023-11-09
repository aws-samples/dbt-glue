import unittest

from dbt.adapters.glue.lakeformation import FilterConfig


class TestLakeFormation(unittest.TestCase):
    def test_lakeformation_filter_api_repr(self) -> None:
        expected_filter = {
            "TableCatalogId": "123456789101",
            "DatabaseName": "some_database",
            "TableName": "some_table",
            "Name": "some_filter",
            "RowFilter": {"FilterExpression": "product_name='Heater'"},
            "ColumnWildcard": {"ExcludedColumnNames": []}
        }
        filter_config = FilterConfig(
            row_filter="product_name='Heater'",
            principals=[],
            column_names=[],
            excluded_column_names=[]
        )
        ret = filter_config.to_api_repr("123456789101", "some_database", "some_table", "some_filter")
        self.assertDictEqual(ret, expected_filter)
