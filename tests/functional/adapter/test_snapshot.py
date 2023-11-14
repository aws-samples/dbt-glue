import pytest

from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import run_dbt, relation_from_name
from tests.util import get_s3_location, get_region, cleanup_s3_location

s3bucket = get_s3_location()
region = get_region()
schema_name = "dbt_functional_test_01"


def check_relation_rows(project, snapshot_name, count):
    relation = relation_from_name(project.adapter, snapshot_name)
    project.run_sql(f"refresh table {relation}")
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == count


class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
                "quote_columns": False,
            },
            "snapshots": {
                "+file_format": "delta",
                "+updated_at": "current_timestamp()",
                "quote_columns": False,
            },
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False
            },
        }

    def test_snapshot_check_cols(self, project):
        # seed commandte
        results = run_dbt(["seed"])
        assert len(results) == 2

        # snapshot command
        results = run_dbt(["snapshot"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 10)
        check_relation_rows(project, "cc_name_snapshot", 10)
        check_relation_rows(project, "cc_date_snapshot", 10)

        relation = relation_from_name(project.adapter, "cc_all_snapshot")
        project.run_sql(f"refresh table {relation}")
        result = project.run_sql(f"select * from {relation}", fetch="all")

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 20)
        check_relation_rows(project, "cc_name_snapshot", 20)
        check_relation_rows(project, "cc_date_snapshot", 20)

    pass


class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

    @pytest.fixture(scope='class', autouse=True)
    def cleanup(self):
        cleanup_s3_location(s3bucket + schema_name, region)
        yield

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
                "quote_columns": False,
            },
            "snapshots": {
                "+file_format": "delta",
                "+updated_at": "current_timestamp()",
                "quote_columns": False,
            },
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": False
            },
        }

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # snapshot has 10 rows
        check_relation_rows(project, "ts_snapshot", 10)

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot now has 20 rows
        check_relation_rows(project, "ts_snapshot", 20)

    pass
