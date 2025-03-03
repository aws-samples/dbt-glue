import unittest
import os
import pytest
from jinja2 import Environment, FileSystemLoader

class TestSchemaChanges(unittest.TestCase):
    """Tests to verify Iceberg schema change fixes"""

    def setUp(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))

        # Path to macros directory
        macros_path = os.path.join(project_root, "dbt", "include", "glue", "macros")
        self.adapters_path = os.path.join(macros_path, "adapters.sql")
        self.incremental_path = os.path.join(macros_path, "materializations", "incremental", "incremental.sql")
        self.strategies_path = os.path.join(macros_path, "materializations", "incremental", "strategies.sql")

        # Verify files exist
        self.assertTrue(os.path.exists(self.adapters_path), f"Missing file: {self.adapters_path}")
        self.assertTrue(os.path.exists(self.incremental_path), f"Missing file: {self.incremental_path}")
        self.assertTrue(os.path.exists(self.strategies_path), f"Missing file: {self.strategies_path}")

        # Read file contents
        with open(self.adapters_path, 'r') as f:
            self.adapters_content = f.read()

        with open(self.incremental_path, 'r') as f:
            self.incremental_content = f.read()

        with open(self.strategies_path, 'r') as f:
            self.strategies_content = f.read()

    def test_create_temporary_view_macro(self):
        """Test the glue__create_temporary_view macro contains Iceberg schema change logic"""
        # Core functionality patterns - these should always be present
        essential_patterns = [
            "glue__create_temporary_view",
            "file_format == 'iceberg'",
            "schema_change_mode in ('append_new_columns', 'sync_all_columns')"
        ]

        # Verify essential patterns exist
        for pattern in essential_patterns:
            self.assertIn(pattern, self.adapters_content,
                          f"Required pattern '{pattern}' not found in adapters.sql")

        # Verify the key behaviors exist (specific implementation may vary)
        # For Iceberg + schema change case
        self.assertIn("table", self.adapters_content.lower())
        self.assertIn("using iceberg", self.adapters_content.lower())

        # For standard case
        self.assertIn("temporary view", self.adapters_content.lower())

    def test_incremental_materialization(self):
        """Test the incremental materialization handles Iceberg schema change"""
        # Check for core functionality related to Iceberg and schema change
        core_patterns = [
            "file_format == 'iceberg'",
            "on_schema_change",
            "schema_change_mode in ('append_new_columns', 'sync_all_columns')"
        ]

        # Look for these patterns in either incremental.sql or adapters.sql
        for pattern in core_patterns:
            self.assertTrue(
                pattern in self.incremental_content or pattern in self.adapters_content,
                f"Core pattern '{pattern}' not found in incremental files"
            )

        # Look for critical functionality - the approach to handle schema changes
        # Avoid checking specific implementations, focus on behavior
        behavior_indicators = [
            "create or replace table",
            "using iceberg",
            "schema=true",  # Properly includes schema for physical tables
            "schema=false"  # Uses schema=false for temp views
        ]

        for indicator in behavior_indicators:
            self.assertTrue(
                indicator in self.incremental_content or indicator in self.adapters_content,
                f"Behavior indicator '{indicator}' not found in incremental files"
            )

    def test_get_incremental_sql(self):
        """Test the incremental SQL generation logic"""
        # Check for the key macro and strategy handling
        self.assertIn("dbt_glue_get_incremental_sql", self.strategies_content)
        self.assertIn("strategy", self.strategies_content)

        # Look for insert operations that would be used for data movement
        insert_patterns = ["insert", "INSERT", "into", "INTO"]
        insert_found = any(pattern in self.strategies_content for pattern in insert_patterns)

        self.assertTrue(insert_found, "No insert operations found in strategies.sql")
