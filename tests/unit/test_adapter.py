from typing import Any, Dict, Optional
import unittest
from unittest import mock
from unittest.mock import Mock
import pytest
from multiprocessing import get_context
import agate.data_types
from botocore.client import BaseClient
from moto import mock_aws

import agate
from dbt.config import RuntimeConfig

import dbt.flags as flags
from dbt.adapters.glue import GlueAdapter
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.relation import SparkRelation
from dbt.adapters.glue.impl import ColumnCsvMappingStrategy
from dbt_common.clients import agate_helper
from tests.util import config_from_parts_or_dicts
from .util import MockAWSService


class TestGlueAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

        self.profile_cfg = {
            "outputs": {
                "test": {
                    "type": "glue",
                    "role_arn": "arn:aws:iam::123456789101:role/GlueInteractiveSessionRole",
                    "region": "us-east-1",
                    "workers": 2,
                    "worker_type": "G.1X",
                    "location": "path_to_location/",
                    "schema": "dbt_unit_test_01",
                    "database": "dbt_unit_test_01",
                    "use_interactive_session_role_for_api_calls": False,
                    "custom_iceberg_catalog_namespace": "custom_iceberg_catalog",
                }
            },
            "target": "test",
        }

    def _get_config(self, **kwargs: Any) -> RuntimeConfig:
        for key, val in kwargs.items():
            self.profile_cfg["outputs"]["test"][key] = val

        return config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)

    def test_glue_connection(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))

        with mock.patch("dbt.adapters.glue.connections.open"):
            connection = adapter.acquire_connection("dummy")
            glueSession: GlueConnection = connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertEqual(connection.type, "glue")
            self.assertEqual(connection.credentials.schema, "dbt_unit_test_01")
            self.assertIsNotNone(connection.handle)
            self.assertIsInstance(glueSession.client, BaseClient)

    @mock_aws
    def test_get_table_type(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))

        database_name = "dbt_unit_test_01"
        table_name = "test_table"
        mock_aws_service = MockAWSService()
        mock_aws_service.create_database(name=database_name)
        mock_aws_service.create_iceberg_table(table_name=table_name, database_name=database_name)
        target_relation = SparkRelation.create(
            schema=database_name,
            identifier=table_name,
        )
        with mock.patch("dbt.adapters.glue.connections.open"):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load
            self.assertEqual(adapter.get_table_type(target_relation), "iceberg_table")

    def test_create_csv_table_slices_big_datasets(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))
        model = {"name": "mock_model", "schema": "mock_schema"}
        session_mock = Mock()
        adapter.get_connection = lambda: (session_mock, "mock_client")
        test_table = agate.Table(
            [(f"mock_value_{i}", f"other_mock_value_{i}") for i in range(2000)], column_names=["value", "other_value"]
        )
        adapter.create_csv_table(model, test_table)

        # test table is between 120000 and 180000 characters so it should be split three times (max chunk is 60000)
        self.assertEqual(session_mock.cursor().execute.call_count, 3)

    def test_get_location(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))
        relation = SparkRelation.create(
            schema="some_database",
            identifier="some_table",
        )
        with mock.patch("dbt.adapters.glue.connections.open"):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load
            self.assertEqual(adapter.get_location(relation), "LOCATION 'path_to_location/some_database/some_table'")

    def test_get_custom_iceberg_catalog_namespace(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))
        with mock.patch("dbt.adapters.glue.connections.open"):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load
            self.assertEqual(adapter.get_custom_iceberg_catalog_namespace(), "custom_iceberg_catalog")

    def test_create_csv_table_provides_schema_and_casts_when_spark_seed_cast_is_enabled(self):
        config = self._get_config()
        config.credentials.enable_spark_seed_casting = True
        adapter = GlueAdapter(config, get_context("spawn"))
        csv_chunks = [{"test_column_double": "1.2345", "test_column_str": "test"}]
        model = {
            "name": "mock_model",
            "schema": "mock_schema",
            "config": {"column_types": {"test_column_double": "double", "test_column_str": "string"}},
        }
        column_mappings = [
            ColumnCsvMappingStrategy("test_column_double", "string", "double"),
            ColumnCsvMappingStrategy("test_column_str", "string", "string"),
        ]
        code = adapter._map_csv_chunks_to_code(csv_chunks, config, model, "True", column_mappings)
        self.assertIn('spark.createDataFrame(csv, "test_column_double: string, test_column_str: string")', code[0])
        self.assertIn(
            'df = df.selectExpr("cast(test_column_double as double) as test_column_double", '
            + '"cast(test_column_str as string) as test_column_str")',
            code[0],
        )

    def test_create_csv_table_doesnt_provide_schema_when_spark_seed_cast_is_disabled(self):
        config = self._get_config()
        config.credentials.enable_spark_seed_casting = False
        adapter = GlueAdapter(config, get_context("spawn"))
        csv_chunks = [{"test_column": "1.2345"}]
        model = {"name": "mock_model", "schema": "mock_schema"}
        column_mappings = [ColumnCsvMappingStrategy("test_column", agate.data_types.Text, "double")]
        code = adapter._map_csv_chunks_to_code(csv_chunks, config, model, "True", column_mappings)
        self.assertIn("spark.createDataFrame(csv)", code[0])


class TestCsvMappingStrategy:
    @pytest.mark.parametrize(
        "agate_type,specified_type,expected_schema_type,expected_cast_type",
        [
            ("timestamp", None, "string", "timestamp"),
            ("double", None, "double", "double"),
            ("bigint", None, "double", "bigint"),
            ("boolean", None, "boolean", "boolean"),
            ("date", None, "string", "date"),
            ("timestamp", None, "string", "timestamp"),
            ("string", None, "string", "string"),
            ("string", "double", "string", "double"),
        ],
        ids=[
            "test isodatetime cast",
            "test number cast",
            "test integer cast",
            "test boolean cast",
            "test date cast",
            "test datetime cast",
            "test text cast",
            "test specified cast",
        ],
    )
    def test_mapping_strategy_provides_proper_mappings(
        self, agate_type, specified_type, expected_schema_type, expected_cast_type
    ):
        column_mapping = ColumnCsvMappingStrategy("test_column", agate_type, specified_type)
        assert column_mapping.as_schema_value() == expected_schema_type
        assert column_mapping.as_cast_value() == expected_cast_type

    def test_from_model_builds_column_mappings(self):
        expected_column_names = ["col_int", "col_str", "col_date", "col_specific"]
        expected_converted_agate_types = [
            "bigint",
            "string",
            "date",
            "string",
        ]
        expected_specified_types = [None, None, None, "double"]
        agate_table = agate.Table(
            [(111, "str_val", "2024-01-01", "1.234")],
            column_names=expected_column_names,
            column_types=[
            agate.data_types.Number(),
            agate.data_types.Text(),
            agate.data_types.Date(),
            agate.data_types.Text(),
        ],
        )
        model = {"name": "mock_model", "config": {"column_types": {"col_specific": "double"}}}
        mappings = ColumnCsvMappingStrategy.from_model(model, agate_table)
        assert expected_column_names == [mapping.column_name for mapping in mappings]
        assert expected_converted_agate_types == [mapping.converted_agate_type for mapping in mappings]
        assert expected_specified_types == [mapping.specified_type for mapping in mappings]
