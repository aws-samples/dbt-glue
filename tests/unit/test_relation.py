import unittest
from unittest.mock import Mock

from dbt.adapters.glue.relation import SparkRelation
from dbt.exceptions import DbtRuntimeError


class TestGlueRelation(unittest.TestCase):
    def test_pre_deserialize(self):
        data = {
            "quote_policy": {
                "database": False,
                "schema": False,
                "identifier": False
            },
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        self.assertEqual(relation.database, "some_database")
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

        data = {
            "quote_policy": {
                "database": False,
                "schema": False,
                "identifier": False
            },
            "path": {
                "database": None,
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        self.assertIsNone(relation.database)
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

        data = {
            "quote_policy": {
                "database": False,
                "schema": False,
                "identifier": False
            },
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        self.assertIsNone(relation.database)
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

    def test_render(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_database",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        self.assertEqual(relation.render(), "some_database.some_table")

        data = {
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        self.assertEqual(relation.render(), "some_schema.some_table")

        data = {
            "path": {
                "database": "some_database",
                "schema": "some_database",
                "identifier": "some_table",
            },
            "include_policy":  {
                "database": True,
                "schema": True,
            },
            "type": None,
        }

        relation = SparkRelation.from_dict(data)
        with self.assertRaises(DbtRuntimeError):
            relation.render()


class TestSparkRelationCreateFrom(unittest.TestCase):
    def _make_quoting(self, quoting_dict):
        quoting = Mock()
        quoting.quoting = quoting_dict
        return quoting

    def _make_relation_config(self, quoting_dict=None):
        relation_config = Mock()
        relation_config.quoting_dict = quoting_dict if quoting_dict is not None else {}
        relation_config.database = "my_db"
        relation_config.schema = "my_schema"
        relation_config.identifier = "my_table"
        relation_config.catalog_name = None
        return relation_config

    def test_none_quoting_in_project_defaults_to_true(self):
        """When dbt core deep_merges None values into the quote policy, they should default to True."""
        quoting = self._make_quoting({"database": None, "schema": None, "identifier": None})
        relation_config = self._make_relation_config()

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertTrue(rel.quote_policy.database)
        self.assertTrue(rel.quote_policy.schema)
        self.assertTrue(rel.quote_policy.identifier)

    def test_explicit_false_quoting_is_preserved(self):
        """When quoting is explicitly set to False in the project, it should not be overridden."""
        quoting = self._make_quoting({"database": False, "schema": False, "identifier": False})
        relation_config = self._make_relation_config()

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertFalse(rel.quote_policy.database)
        self.assertFalse(rel.quote_policy.schema)
        self.assertFalse(rel.quote_policy.identifier)

    def test_explicit_true_quoting_is_preserved(self):
        """When quoting is explicitly set to True in the project, it should remain True."""
        quoting = self._make_quoting({"database": True, "schema": True, "identifier": True})
        relation_config = self._make_relation_config()

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertTrue(rel.quote_policy.database)
        self.assertTrue(rel.quote_policy.schema)
        self.assertTrue(rel.quote_policy.identifier)

    def test_mixed_none_and_explicit_quoting(self):
        """None fields default to True while explicitly set fields are preserved."""
        quoting = self._make_quoting({"database": None, "schema": False, "identifier": True})
        relation_config = self._make_relation_config()

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertTrue(rel.quote_policy.database)   # None → defaulted to True
        self.assertFalse(rel.quote_policy.schema)    # explicitly False → preserved
        self.assertTrue(rel.quote_policy.identifier) # explicitly True → preserved

    def test_none_quoting_in_config_defaults_to_true(self):
        """When relation_config.quoting_dict contains None values, they should default to True."""
        quoting = self._make_quoting({})
        relation_config = self._make_relation_config(
            quoting_dict={"database": None, "schema": None, "identifier": None}
        )

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertTrue(rel.quote_policy.database)
        self.assertTrue(rel.quote_policy.schema)
        self.assertTrue(rel.quote_policy.identifier)

    def test_explicit_false_in_config_is_preserved(self):
        """When relation_config.quoting_dict explicitly sets False, it should not be overridden."""
        quoting = self._make_quoting({})
        relation_config = self._make_relation_config(
            quoting_dict={"database": False, "schema": False, "identifier": False}
        )

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertFalse(rel.quote_policy.database)
        self.assertFalse(rel.quote_policy.schema)
        self.assertFalse(rel.quote_policy.identifier)

    def test_relation_path_is_set_correctly(self):
        """Ensures create_from correctly passes through database/schema/identifier."""
        quoting = self._make_quoting({})
        relation_config = self._make_relation_config()

        rel = SparkRelation.create_from(quoting, relation_config)

        self.assertEqual(rel.database, "my_db")
        self.assertEqual(rel.schema, "my_schema")
        self.assertEqual(rel.identifier, "my_table")
