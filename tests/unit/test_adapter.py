from typing import Any, Dict, Optional
import unittest
from unittest import mock
from unittest.mock import Mock
from multiprocessing import get_context
from botocore.client import BaseClient
from moto import mock_aws
import boto3

import agate
from dbt.config import RuntimeConfig

import dbt.flags as flags
from dbt.adapters.glue import GlueAdapter
from dbt.adapters.glue.gluedbapi import GlueConnection
from dbt.adapters.glue.relation import SparkRelation
from dbt.adapters.contracts.relation import RelationConfig
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
                    "location" : "path_to_location/",
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
        adapter.get_connection = lambda: (session_mock, 'mock_client')
        test_table = agate.Table([(f'mock_value_{i}',f'other_mock_value_{i}') for i in range(2000)], column_names=['value', 'other_value'])
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
            print(adapter.get_location(relation))
            self.assertEqual(adapter.get_location(relation), "LOCATION 'path_to_location/some_database/some_table'")
    
    def test_get_custom_iceberg_catalog_namespace(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))
        with mock.patch("dbt.adapters.glue.connections.open"):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load
            self.assertEqual(adapter.get_custom_iceberg_catalog_namespace(), "custom_iceberg_catalog")

    @mock_aws
    def test_when_database_not_exists_list_relations_without_caching_returns_empty_array(self):
        config = self._get_config()
        adapter = GlueAdapter(config, get_context("spawn"))
        adapter.get_connection = lambda : (None, boto3.client("glue", region_name="us-east-1"))
        relation = Mock(SparkRelation)
        relation.schema = 'mockdb'
        actual = adapter.list_relations_without_caching(relation)
        self.assertEqual([],actual)

    @mock_aws
    def test_list_relations_returns_database_tables(self):
        config = self._get_config()
        glue_client = boto3.client("glue", region_name="us-east-1")

        # Prepare database tables
        database_name = 'mockdb'
        table_names = ['table1', 'table2', 'table3']
        glue_client.create_database(DatabaseInput={"Name":database_name})
        for table_name in table_names:
            glue_client.create_table(DatabaseName=database_name,TableInput={"Name":table_name})
        expected = [(database_name, table_name) for table_name in table_names]

        # Prepare adapter for test
        adapter = GlueAdapter(config, get_context("spawn"))
        adapter.get_connection = lambda : (None, glue_client)
        relation = Mock(SparkRelation)
        relation.schema = database_name

        relations = adapter.list_relations_without_caching(relation)

        actual = [(relation.path.schema, relation.path.identifier) for relation in relations]
        self.assertCountEqual(expected,actual)
