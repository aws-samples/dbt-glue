from base import GlueMacroTestBase
import pytest
from unittest.mock import Mock, MagicMock

class TestGlueMacros(GlueMacroTestBase):
    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros"]

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters.sql"

    @pytest.fixture(scope="class")
    def default_context(self) -> dict:
        context = {
            "validation": Mock(),
            "model": Mock(),
            "exceptions": Mock(),
            "config": Mock(),
            "statement": lambda r, caller: r,
            "adapter": Mock(),
            "var": Mock(),
            "return": lambda r: r,
            "is_incremental": Mock(return_value=False),
            "partition_cols": lambda *args, **kwargs: "",
            "clustered_cols": lambda *args, **kwargs: "",
            "comment_clause": lambda *args, **kwargs: "",
            "set_table_properties": lambda *args, **kwargs: "",
            "create_temporary_view": lambda *args, **kwargs: "",
            "glue__location_clause": lambda *args, **kwargs: "",
            "glue__file_format_clause": lambda *args, **kwargs: "using {} ".format(
                context["config"].get("file_format", "parquet")
            ),
        }

        # Mock model macro calls
        mock_model = MagicMock()
        mock_model.alias = "test_table"
        context["model"] = mock_model

        # Mock config.get calls
        local_config = {}
        mock_config = MagicMock()
        mock_config.get = lambda key, default=None, **kwargs: local_config.get(key, default)
        context["config"] = mock_config

        return context

    def test_create_table_as_with_timestamp_enabled(self, template, relation, config):
        """Test table creation with timestamp column enabled"""
        config.update({
            'file_format': 'iceberg',
            'add_iceberg_timestamp': True
        })

        sql = "select 1 as id, 'test' as name"
        result = self.run_macro(template, "glue__create_table_as", False, relation, sql)

        # Check if the result contains necessary elements
        assert "create or replace table" in result.lower()
        assert "using iceberg" in result.lower()
        assert "current_timestamp() as update_iceberg_ts" in result.lower()

    def test_create_table_as_with_timestamp_disabled(self, template, relation, config):
        """Test table creation with timestamp column disabled"""
        config.update({
            'file_format': 'iceberg',
            'add_iceberg_timestamp': False
        })

        sql = "select 1 as id, 'test' as name"
        result = self.run_macro(template, "glue__create_table_as", False, relation, sql)

        # Check if timestamp column is not added
        assert "create or replace table" in result.lower()
        assert "using iceberg" in result.lower()
        assert "update_iceberg_ts" not in result.lower()

    def test_create_table_as_non_iceberg(self, template, relation, config):
        """Test table creation with non-iceberg format"""
        config.update({
            'file_format': 'parquet',
            'add_iceberg_timestamp': True  # This option should be ignored for non-iceberg format
        })

        sql = "select 1 as id, 'test' as name"
        result = self.run_macro(template, "glue__create_table_as", False, relation, sql)

        # Check if timestamp is not added for non-iceberg format
        assert "create table" in result.lower()
        assert "using parquet" in result.lower()
        assert "update_iceberg_ts" not in result.lower()

    def test_file_format_defaults(self, template):
        """Test default value for file format"""
        result = self.run_macro(template, "glue__file_format_clause")
        assert "using parquet" in result.lower()  # parquet is default