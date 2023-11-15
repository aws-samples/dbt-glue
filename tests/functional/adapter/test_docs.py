import random
import string
import pytest
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.basic.expected_catalog import no_stats

database_suffix = ''.join(random.choices(string.digits, k=4))
schema_name = f"dbt_functional_test_docs_{database_suffix}"


class TestDocsGenerate(BaseDocsGenerate):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["database"] = unique_schema
        outputs["default"]["schema"] = unique_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        role = None
        id_type = "double"
        text_type = "string"
        time_type = "string"
        view_type = "view"
        table_type = "table"
        model_stats = no_stats()
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

        my_schema_name = case(project.test_schema)

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
                        "database": my_schema_name,
                        "name": case("seed"),
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": seed_stats,
                    "columns": seed_columns,
                },
                "model.test.model": {
                    "unique_id": "model.test.model",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": my_schema_name,
                        "name": case("model"),
                        "type": view_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": model_stats,
                    "columns": seed_columns,
                },
            },
            "sources": {}
        }

    pass


class TestDocsGenReferencesGlue(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["database"] = unique_schema
        outputs["default"]["schema"] = unique_schema
        return {"test": {"outputs": outputs, "target": "default"}}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        role = None
        id_type = "double"
        text_type = "string"
        time_type = "string"
        view_type = "view"
        table_type = "table"
        model_stats = no_stats()
        bigint_type = "bigint"
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

        my_schema_name = case(project.test_schema)

        summary_columns = {
            "first_name": {
                "name": "first_name",
                "index": 0,
                "type": text_type,
                "comment": None,
            },
            "ct": {
                "name": "ct",
                "index": 0,
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
                        "database": my_schema_name,
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
                        "database": my_schema_name,
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
                        "database": my_schema_name,
                        "name": case("view_summary"),
                        "type": view_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": view_summary_stats,
                    "columns": summary_columns,
                },
            },
            "sources": {}
        }

        pass
