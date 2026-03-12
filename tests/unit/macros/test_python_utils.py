import unittest
from unittest.mock import Mock


class TestDbtObjRefSource(unittest.TestCase):
    def setUp(self):
        self.refs = {"stg_orders": "analytics.stg_orders"}
        self.sources = {"raw_alias.orders": "production_raw.orders"}
        self.table_fn = Mock(side_effect=lambda t: f"DF<{t}>")

        # Mirrors the generated code in python_utils.sql
        _dbt_core_ref = self._mock_core_ref
        _dbt_core_source = self._mock_core_source
        table_fn = self.table_fn

        class dbtObj:
            def __init__(self):
                self.table_function = table_fn

            def ref(self, *args, **kwargs):
                return _dbt_core_ref(
                    *args, **kwargs, dbt_load_df_function=self.table_function
                )

            def source(self, *args):
                return _dbt_core_source(*args, dbt_load_df_function=self.table_function)

        self.dbt = dbtObj()

    def _mock_core_ref(self, *args, **kwargs):
        key = ".".join(args)
        version = kwargs.get("v") or kwargs.get("version")
        if version:
            key += f".v{version}"
        return kwargs["dbt_load_df_function"](self.refs[key])

    def _mock_core_source(self, *args, dbt_load_df_function):
        key = ".".join(args)
        return dbt_load_df_function(self.sources[key])

    def test_ref(self):
        result = self.dbt.ref("stg_orders")
        self.table_fn.assert_called_once_with("analytics.stg_orders")
        self.assertEqual(result, "DF<analytics.stg_orders>")

    def test_ref_with_version(self):
        self.refs["stg_orders.v2"] = "analytics.stg_orders_v2"
        self.dbt.ref("stg_orders", v=2)
        self.table_fn.assert_called_once_with("analytics.stg_orders_v2")

    def test_source_resolves_schema_not_alias(self):
        """The core bug: source alias != schema. dbt-core resolves to the real schema."""
        result = self.dbt.source("raw_alias", "orders")
        self.table_fn.assert_called_once_with("production_raw.orders")
        self.assertEqual(result, "DF<production_raw.orders>")
