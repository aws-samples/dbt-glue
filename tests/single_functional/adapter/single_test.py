import pytest
import os

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats, expected_references_catalog
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt, rm_file, get_artifact, check_datetime_between

class TestDocsGenerate(BaseDocsGenerate):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return "dbt_functional_test_01"

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        role = None
        id_type = "double"
        text_type = "string"
        time_type = "string"
        view_type = "view"
        table_type = "table"
        model_stats = no_stats()
        bigint_type = None
        seed_stats = None
        case = None
        case_columns = False
        view_summary_stats = None

        if case is None:
            def case(x):
                return x

        col_case = case if case_columns else lambda x: x

        if seed_stats is None:
            seed_stats = model_stats

        if view_summary_stats is None:
            view_summary_stats = model_stats

        model_database = project.database
        my_schema_name = case(project.test_schema)

        summary_columns = {
            "first_name": {
                "name": "first_name",
                "index": 1,
                "type": text_type,
                "comment": None,
            },
            "ct": {
                "name": "ct",
                "index": 2,
                "type": bigint_type,
                "comment": None,
            },
        }

        seed_columns = {
            "id": {
                "name": col_case("id"),
                "index": 0,
                "type": id_type,
                "comment": None,
            },
            "first_name": {
                "name": col_case("first_name"),
                "index": 0,
                "type": text_type,
                "comment": None,
            },
            "email": {
                "name": col_case("email"),
                "index": 0,
                "type": text_type,
                "comment": None,
            },
            "ip_address": {
                "name": col_case("ip_address"),
                "index": 0,
                "type": text_type,
                "comment": None,
            },
            "updated_at": {
                "name": col_case("updated_at"),
                "index": 0,
                "type": time_type,
                "comment": None,
            },
        }
        return {
            "nodes": {
                "seed.test.seed": {
                    "unique_id": "seed.test.seed",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": project.database,
                        "name": case("seed"),
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": seed_stats,
                    "columns": seed_columns,
                },
                "model.test.ephemeral_summary": {
                    "unique_id": "model.test.ephemeral_summary",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": model_database,
                        "name": case("ephemeral_summary"),
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": model_stats,
                    "columns": summary_columns,
                },
                "model.test.view_summary": {
                    "unique_id": "model.test.view_summary",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": model_database,
                        "name": case("view_summary"),
                        "type": view_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": view_summary_stats,
                    "columns": summary_columns,
                },
            },
            "sources": {
                "source.test.my_source.my_table": {
                    "unique_id": "source.test.my_source.my_table",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": project.database,
                        "name": case("seed"),
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": seed_stats,
                    "columns": seed_columns,
                },
            },
        }

    pass

