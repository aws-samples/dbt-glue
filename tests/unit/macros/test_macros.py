import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader


class TestGlueMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("dbt/include/glue/macros"),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.config = {}
        self.return_value = None
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "target": mock.Mock(),
            "return": self._capture_return,
            "this": mock.Mock(),
            "add_iceberg_timestamp_column": lambda sql: sql,
            "make_temp_relation": mock.Mock(),
            "set_table_properties": lambda props: f"TBLPROPERTIES ({props})" if props != {} else "",
            "comment_clause": lambda: "",
            "partition_cols": lambda label: f"{label} (part_col)" if label else "",
            "clustered_cols": lambda label: "",
            "get_assert_columns_equivalent": lambda sql: "",
            "sql_header": None,
            "get_merge_update_columns": lambda update_cols, exclude_cols, dest_cols: update_cols if update_cols else ["col1", "col2"],
            "incremental_validate_on_schema_change": lambda on_schema_change, default: default,
            "process_schema_changes": lambda on_schema_change, tmp_relation, target_relation: None,
            "run_hooks": lambda hooks: None,
            "should_full_refresh": lambda: False,
            "dbt_glue_validate_get_file_format": lambda raw_file_format: raw_file_format,
            "dbt_glue_validate_get_incremental_strategy": lambda raw_strategy, file_format: raw_strategy,
            "log": lambda msg, info=False: print(f"LOG: {msg}"),
        }

        # Configure mocks
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        self.default_context["adapter"].get_location = lambda rel: "location '/path/to/data'"
        self.default_context["adapter"].get_columns_in_relation = lambda rel: [
            mock.Mock(name="id"),
            mock.Mock(name="name")
        ]
        self.default_context["make_temp_relation"] = lambda base, suffix: mock.Mock(
            include=lambda schema: f"temp_relation_{suffix}" if not schema else f"schema.temp_relation_{suffix}"
        )

        # For drop_relation
        self.default_context["this"].type = "table"

        # For incremental testing
        self.default_context["this"].include = lambda schema: "test_table" if not schema else "test_schema.test_table"

        # Mock exceptions.raise_compiler_error
        self.default_context["exceptions"].raise_compiler_error = lambda msg: f"mock.raise_compiler_error({msg})"

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, *args, **kwargs):
        """Run a macro with the given template and arguments"""
        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"glue__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        macro = getattr(template.module, name)
        value = macro(*args, **kwargs)
        value = re.sub(r"\s\s+", " ", value)
        
        # If return_value was set, use it; otherwise, use rendered output
        if (value == " " or value.isspace()) and self.return_value is not None:
            value = self.return_value
        
        self.return_value = None

        return value

    def _capture_return(self, value):
        self.return_value = value
        return value
    
    def test_macros_load(self):
        """Test that macros can be loaded"""
        self.jinja_env.get_template("adapters.sql")
        # Skip testing materialization directly since it requires custom extensions
        # self.jinja_env.get_template("materializations/incremental/incremental.sql")
        self.jinja_env.get_template("materializations/incremental/strategies.sql")

    def test_glue_create_table_as(self):
        """Test the basic create table as functionality"""
        template = self.__get_template("adapters.sql")
        relation = mock.Mock()
        relation.identifier = "my_table"
        relation.schema = "my_schema"

        sql = self.__run_macro(
            template, "glue__create_table_as", False, relation, "select 1 as id"
        ).strip()

        # Default is to create a table using PARQUET
        self.assertIn("create table", sql)
        self.assertIn("using PARQUET", sql)
        self.assertIn("as select 1 as id", sql)

    def test_glue_create_table_as_with_file_format(self):
        """Test create table as with different file formats"""
        template = self.__get_template("adapters.sql")
        relation = mock.Mock()
        relation.identifier = "my_table"
        relation.schema = "my_schema"

        self.default_context["glue__make_target_relation"] = mock.Mock(return_value="MOCK_FULL_RELATION")

        # Test with Delta format
        self.config["file_format"] = "delta"
        sql = self.__run_macro(
            template, "glue__create_table_as", False, relation, "select 1 as id"
        ).strip()
        self.assertIn("create or replace table", sql)
        self.assertIn("using delta", sql)

        # Test with Iceberg format
        self.config["file_format"] = "iceberg"
        sql = self.__run_macro(
            template, "glue__create_table_as", False, relation, "select 1 as id"
        ).strip()
        self.assertIn("create or replace table", sql)
        self.assertIn("using iceberg", sql)

        # Test with Hudi format
        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "glue__create_table_as", False, relation, "select 1 as id"
        ).strip()
        self.assertIn("create table", sql)
        self.assertIn("using hudi", sql)

    def test_glue_create_temporary_view(self):
        """Test temporary view creation with different schema change modes"""
        template = self.__get_template("adapters.sql")
        
        def relation_to_str(self):
            return f"{self.schema}.{self.identifier}" if self.schema != "" else f"{self.identifier}"

        relation = mock.Mock()
        relation.schema = "my_schema"
        relation.identifier = "my_view"
        relation.type = "view"
        relation.__str__ = relation_to_str
        relation.include = lambda schema: f"{relation.identifier}" if not schema else f"{relation.schema}.{relation.identifier}"
        relation.is_view = lambda self: (self.type == "view")

        self.default_context["glue__make_target_relation"] = lambda relation, file_format: f"{relation.schema}.{relation.identifier}"
        
        # Standard temporary view
        sql = self.__run_macro(
            template, "glue__create_temporary_view", relation, "MY_QUERY"
        ).strip()
        self.assertEqual(sql, "create or replace temporary view my_view as MY_QUERY")

        # Skipping Iceberg pattern as glue__make_target_relation does not work in unit test
        self.config["file_format"] = "iceberg"
        self.config["on_schema_change"] = "append_new_columns"
        relation.identifier = "my_table"
        relation.type = "table"
        sql = self.__run_macro(
            template, "glue__create_temporary_view", relation, "MY_QUERY"
        ).strip()
        self.assertEqual(sql, "create or replace table my_schema.my_table using iceberg as MY_QUERY")

    def test_glue_get_drop_sql(self):
        """Test the SQL generation for dropping relations"""
        template = self.__get_template("adapters.sql")
        target_relation_template = self.__get_template("utils/make_target_relation.sql")
        
        def relation_to_str(self):
            return f"{self.schema}.{self.identifier}" if self.schema != "" else f"{self.identifier}"

        relation = mock.Mock()
        relation.schema = "relation_schema"
        relation.identifier = "relation_entity"
        relation.type = "default"
        relation.__str__ = relation_to_str
        relation.include = lambda schema: f"{relation.identifier}" if not schema else f"{relation.schema}.{relation.identifier}"
        relation.is_view = lambda self: (self.type == "view")
             
        # Enhanced incorporate to return a new mock with updated schema/identifier
        def incorporate_side_effect(*, path=None, type=None, **kwargs):
            new_relation = mock.Mock()
            new_relation.schema = path.get("schema", relation.schema) if path else relation.schema
            new_relation.identifier = path.get("identifier", relation.identifier) if path else relation.identifier
            new_relation.type = type if type else relation.type
            new_relation.__str__ = relation_to_str
            new_relation.is_view = lambda self: (self.type == "view")
        
            new_include_relation = mock.Mock()
            new_include_relation.schema = ""
            new_include_relation.identifier = path.get("identifier", new_relation.identifier) if path else new_relation.identifier
            new_include_relation.type = new_relation.type
            new_include_relation.__str__ = relation_to_str
            new_include_relation.is_view = lambda self: (self.type == "view")

            new_relation.include=lambda schema: new_include_relation if not schema else new_relation
            
            return new_relation

        relation.incorporate.side_effect = incorporate_side_effect

        # ---------------------------------------------
        # This allows us to mock macros and objects that are not defined in the macro template itself
        # ---------------------------------------------
        self.config["file_format"] = "iceberg"
        self.default_context["target"].schema = 'profile_schema'
        self.default_context["adapter"].get_custom_iceberg_catalog_namespace = mock.Mock(return_value="iceberg_catalog")
        
        # ---------------------------------------------
        # This allows us to mock nested macro calls as long as the nested macro
        # Has the base definition macro calling the adapter macro with adapter.dispatch inside it
        # And the reason we want to do that for glue__make_target_relation is that it uses 
        # "do return()" logic, which is handled differently in DBT than Jinja and our mocks use Jinja.
        # Without mocking, it only returns standard Jinja output, not the value from do return().
        # So we mock it to get that behavior and we mock by calling it explicitly here first.
        # Being careful not to have a circular loop!
        # ---------------------------------------------
        def make_target_relation(relation, file_format):
            target_relation = self.__run_macro(
                target_relation_template, "glue__make_target_relation", relation, file_format
            )
            return target_relation
        
        self.default_context["glue__make_target_relation"] = make_target_relation # mock.Mock(return_value="MOCK_FULL_RELATION")

        # ---------------------------------------------
        # TESTS
        # ---------------------------------------------

        # TEST 1: For temporary views
        relation.schema = ""
        relation.identifier = "temporary_entity"
        relation.type = "view"
        relation.is_view = True
        expected_full_relation = make_target_relation(relation, self.config["file_format"])
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop view if exists {expected_full_relation}", sql)

        # TEST 2: 
        # - Not a temporary view 
        # - Iceberg or s3tables format
        # - Purge dropped iceberg data true
        # - Is a view, but since iceberg/s3tables, we drop as table
        self.config["purge_dropped_iceberg_data"] = 'True'
        relation.schema = "relation_schema"
        relation.identifier = "relation_entity"
        relation.type = "view"
        relation.is_view = True
        expected_full_relation = make_target_relation(relation, self.config["file_format"])
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop table if exists {expected_full_relation} purge", sql)

        # TEST 3: 
        # - Not a temporary view 
        # - Iceberg or s3tables format
        # - NOT Purge dropped iceberg data true
        # - Is a view, but since iceberg/s3tables, we drop as table
        self.config["purge_dropped_iceberg_data"] = 'False'
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop table if exists {expected_full_relation}", sql)

        # TEST 4: 
        # - Not a temporary view 
        # - NOT Iceberg or s3tables format
        # - Is a view, but since NOT iceberg/s3tables, we drop as VIEW
        self.config["file_format"] = 'parquet'
        expected_full_relation = make_target_relation(relation, self.config["file_format"])
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop view if exists {expected_full_relation}", sql)

        # TEST 5: 
        # - NOT A VIEW
        # - Iceberg or s3tables format
        # - Purge dropped iceberg data true
        relation.type = "table"
        relation.is_view = False
        self.config["file_format"] = 's3tables'
        self.config["purge_dropped_iceberg_data"] = 'True'
        expected_full_relation = make_target_relation(relation, self.config["file_format"])
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop table if exists {expected_full_relation} purge", sql)

        # TEST 6: 
        # - NOT A VIEW
        # - NOT Iceberg or s3tables format
        # - OR NOT Purge dropped iceberg data true
        relation.type = "table"
        relation.is_view = False
        self.config["file_format"] = 'parquet'
        self.config["purge_dropped_iceberg_data"] = 'True'
        expected_full_relation = make_target_relation(relation, self.config["file_format"])
        
        sql = self.__run_macro(
            template, "glue__get_drop_sql", relation
        ).strip()

        self.assertTrue(True)
        self.assertIn(f"drop table if exists {expected_full_relation}", sql)

    def test_glue_make_target_relation(self):
        """Test target relation creation for Iceberg"""
        template = self.__get_template("utils/make_target_relation.sql")

        relation = mock.Mock()
        
        def relation_to_str(self):
            return f"{self.schema}.{self.identifier}" if self.schema != "" else f"{self.identifier}"

        # Enhanced incorporate to return a new mock with updated schema/identifier
        def incorporate_side_effect(*, path=None, type=None, **kwargs):
            new_relation = mock.Mock()
            new_relation.schema = path.get("schema", relation.schema) if path else relation.schema
            new_relation.identifier = path.get("identifier", relation.identifier) if path else relation.identifier
            new_relation.type = type if type else relation.type
            new_relation.__str__ = relation_to_str
            new_relation.is_view = lambda self: (relation.type == "view")
        
            new_include_relation = mock.Mock()
            new_include_relation.schema = ""
            new_include_relation.identifier = path.get("identifier", new_relation.identifier) if path else new_relation.identifier
            new_include_relation.type = new_relation.type
            new_include_relation.__str__ = relation_to_str
            new_include_relation.is_view = lambda self: (self.type == "view")

            new_relation.include=lambda schema: new_include_relation if not schema else new_relation
            
            return new_relation

        relation.incorporate.side_effect = incorporate_side_effect

        relation.schema = "test_schema"
        relation.identifier = "test_table"
        relation.type = "default"
        relation.__str__ = relation_to_str
        relation.include = lambda schema: f"{relation.identifier}" if not schema else f"{relation.schema}.{relation.identifier}"
        relation.is_view = lambda self: (self.type == "view")
        
        # Setup for Iceberg with custom catalog
        self.default_context["adapter"].get_custom_iceberg_catalog_namespace = mock.Mock(return_value="iceberg_catalog")

        # Test as temporary view
        relation.schema = ""
        relation.type = "view"
        target_relation = self.__run_macro(
            template, "glue__make_target_relation", relation, "iceberg"
        )
        self.assertEqual(str(target_relation), "test_table")

        # Test with Iceberg and schema has iceberg_catalog in schema
        relation.schema = "iceberg_catalog.test_schema"
        relation.type = "table"
        target_relation = self.__run_macro(
            template, "glue__make_target_relation", relation, "iceberg"
        )
        self.assertEqual(str(target_relation), "iceberg_catalog.test_schema.test_table")

        # Test with Iceberg and schema does NOT have iceberg_catalog in schema
        relation.schema = "test_schema"
        relation.type = "table"
        target_relation = self.__run_macro(
            template, "glue__make_target_relation", relation, "iceberg"
        )
        relation.incorporate.assert_called_once()
        self.assertEqual(str(target_relation), "iceberg_catalog.test_schema.test_table")

        # Test with Non-Iceberg and schema does NOT have iceberg_catalog in schema
        target_relation = self.__run_macro(
            template, "glue__make_target_relation", relation, "parquet"
        )
        self.assertEqual(str(target_relation), "test_schema.test_table")

    def test_glue_make_temp_relation(self):
        """Test temp relation creation for Iceberg"""
        template = self.__get_template("adapters.sql")
        target_relation = mock.Mock()
        target_relation.schema = "target_schema"
        target_relation.identifier = "target_table"
        temp_relation_suffix = "_test_tmp"

        # Enhanced incorporate to return a new mock with updated schema/identifier
        def incorporate_side_effect(*, path=None, type, **kwargs):
            new_relation = mock.Mock()
            new_relation.schema = path.get("schema", target_relation.schema) if path else target_relation.schema
            new_relation.identifier = path.get("identifier", target_relation.identifier) if path else target_relation.identifier
            new_relation.type = type if type else "default"

            new_include_relation = mock.Mock()
            new_include_relation.schema = ""
            new_include_relation.identifier = path.get("identifier", target_relation.identifier) if path else target_relation.identifier
            new_include_relation.type = type if type else "default"
            
            new_relation.include=lambda schema: new_include_relation if not new_relation.schema else new_relation
            
            return new_relation

        target_relation.incorporate.side_effect = incorporate_side_effect

        # Setup for Iceberg with custom catalog
        self.default_context["adapter"].get_custom_iceberg_catalog_namespace = mock.Mock(return_value="iceberg_catalog")

        # Setup for temp schema being set in target profile
        self.default_context["target"].temp_schema = 'test_schema'
        
        # Test that temp relation is created with temp schema as provided in target profile
        temp_relation = self.__run_macro(
            template, "glue__make_temp_relation", target_relation, temp_relation_suffix
        )
        target_relation.incorporate.assert_called_once()

        # Verify it still returns something without throwing an exception
        self.assertTrue(True)

        self.assertNotEqual(temp_relation.schema, target_relation.schema)
        self.assertEqual(temp_relation.schema, self.default_context["target"].temp_schema)
        self.assertEqual(temp_relation.identifier, target_relation.identifier + temp_relation_suffix)

        # Setup for temp schema being set in target profile
        self.default_context["target"].temp_schema = None

        # Test that temp relation is created with target relation schema when temp_schema not provided in target profile
        temp_relation = self.__run_macro(
            template, "glue__make_temp_relation", target_relation, temp_relation_suffix
        )

        self.assertNotEqual(temp_relation.schema, self.default_context["target"].temp_schema)
        self.assertEqual(temp_relation.schema, target_relation.schema)
        self.assertEqual(temp_relation.identifier, target_relation.identifier + temp_relation_suffix)

    def test_simple_get_insert_strategies(self):
        """Test the simple insert strategies separately"""
        template = self.__get_template("materializations/incremental/strategies.sql")
        source = mock.Mock()
        source.include = lambda schema: "source_view" if not schema else "schema.source_view"
        target = mock.Mock()
        target.include = lambda schema: "target_table" if not schema else "schema.target_table"

        # Test append strategy directly
        sql = self.__run_macro(
            template, "get_insert_into_sql", source, target
        ).strip()
        self.assertIn("insert into table", sql)

        # Test insert_overwrite strategy directly
        sql = self.__run_macro(
            template, "get_insert_overwrite_sql", source, target
        ).strip()
        self.assertIn("insert overwrite table", sql)
        self.assertIn("set hive.exec.dynamic.partition.mode=nonstrict", sql)
