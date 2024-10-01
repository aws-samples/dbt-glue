from typing import Any, Dict, Optional
import unittest
from unittest import mock
from unittest.mock import Mock
from multiprocessing import get_context
from botocore.client import BaseClient
from moto import mock_aws

import agate
from dbt.config import RuntimeConfig

import dbt.flags as flags
from dbt.adapters.glue import GlueAdapter
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.relation import SparkRelation
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
                    "schema": "dbt_unit_test_01",
                    "database": "dbt_unit_test_01",
                    "use_interactive_session_role_for_api_calls": False
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
        adapter.get_connection = lambda: (session_mock, 'mock_client')
        test_table = agate.Table([(f'mock_value_{i}',f'other_mock_value_{i}') for i in range(2000)], column_names=['value', 'other_value'])
        adapter.create_csv_table(model, test_table)

        # test table is between 120000 and 180000 characters so it should be split three times (max chunk is 60000)
        self.assertEqual(session_mock.cursor().execute.call_count, 3)