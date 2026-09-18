import pytest
from dbt.tests.util import run_dbt

# The source table is produced by a model in the same project, so the freshness
# run has something to look at in the Glue catalog.
seed_model_sql = """
{{ config(materialized='table') }}

select 1 as id, current_timestamp() as loaded_at
"""


column_based_source_yml = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    database: "{{ target.schema }}"
    tables:
      - name: freshness_seed
        loaded_at_field: loaded_at
        freshness:
          warn_after: {count: 1, period: day}
          error_after: {count: 30, period: day}
"""


metadata_based_source_yml = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    database: "{{ target.schema }}"
    tables:
      - name: freshness_seed
        freshness:
          warn_after: {count: 1, period: day}
          error_after: {count: 30, period: day}
"""


class BaseSourceFreshnessGlue:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "freshness_seed.sql": seed_model_sql,
            "sources.yml": self.source_yml,
        }

    def test_source_freshness_passes_for_a_table_just_written(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["source", "freshness"])

        assert len(results) == 1
        assert results[0].status == "pass"
        assert results[0].max_loaded_at is not None
        assert results[0].age >= 0


class TestSourceFreshnessLoadedAtFieldGlue(BaseSourceFreshnessGlue):
    """loaded_at_field freshness: reads the data through the glue session."""

    source_yml = column_based_source_yml


class TestSourceFreshnessFromMetadataGlue(BaseSourceFreshnessGlue):
    """No loaded_at_field: freshness comes from the Glue Data Catalog."""

    source_yml = metadata_based_source_yml
