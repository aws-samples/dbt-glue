import pytest

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
from dbt.tests.adapter.basic.files import (
    schema_base_yml
)
from dbt.tests.util import run_dbt, update_rows, relation_from_name
from dbt.tests.adapter.basic.test_snapshot_check_cols import check_relation_rows

# To test
#class TestDocsGenerate(BaseDocsGenerate):
#    pass


#class TestDocsGenReferences(BaseDocsGenReferences):
#    pass


schema_name = "dbt_functional_test_01"


class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return schema_name

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
        # seed command
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
        result = project.run_sql(f"select * from {relation}", fetch="all")

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 20)
        check_relation_rows(project, "cc_name_snapshot", 20)
        check_relation_rows(project, "cc_date_snapshot", 20)

        # # update some timestamps in the "added" seed so the snapshot sees 10 more new rows
        # update_rows_config = {
        #     "name": "added",
        #     "dst_col": "some_date",
        #     "clause": {"src_col": "some_date", "type": "add_timestamp"},
        #     "where": "id > 10 and id < 21",
        # }
        # update_rows(project.adapter, update_rows_config)
        #
        # # re-run snapshots, using "added'
        # results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        # for result in results:
        #     assert result.status == "success"
        #
        # # check rowcounts for all snapshots
        # check_relation_rows(project, "cc_all_snapshot", 30)
        # check_relation_rows(project, "cc_date_snapshot", 30)
        # # unchanged: only the timestamp changed
        # check_relation_rows(project, "cc_name_snapshot", 20)
        #
        # # Update the name column
        # update_rows_config = {
        #     "name": "added",
        #     "dst_col": "name",
        #     "clause": {
        #         "src_col": "name",
        #         "type": "add_string",
        #         "value": "_updated",
        #     },
        #     "where": "id < 11",
        # }
        # update_rows(project.adapter, update_rows_config)
        #
        # # re-run snapshots, using "added'
        # results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        # for result in results:
        #     assert result.status == "success"
        #
        # # check rowcounts for all snapshots
        # check_relation_rows(project, "cc_all_snapshot", 40)
        # check_relation_rows(project, "cc_name_snapshot", 30)
        # # does not see name updates
        # check_relation_rows(project, "cc_date_snapshot", 30)

    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass