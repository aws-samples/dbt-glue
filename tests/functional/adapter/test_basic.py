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

class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return "dbt_functional_test_01"

    pass


class TestSingularTestsGlue(BaseSingularTests):
    pass


class TestEmptyGlue(BaseEmpty):
    pass


class TestEphemeralGlue(BaseEphemeral):
    # all tests within this test has the same schema
    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return "dbt_functional_test_01"

    pass

class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
    pass

class TestIncrementalGlue(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        model_incremental = """
           select * from {{ source('raw', 'seed') }}
           """.strip()

        return {"incremental.sql": model_incremental, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def unique_schema(request, prefix) -> str:
        return "dbt_functional_test_01"
    pass


class TestGenericTestsGlue(BaseGenericTests):
    pass

# To test
#class TestDocsGenerate(BaseDocsGenerate):
#    pass


#class TestDocsGenReferences(BaseDocsGenReferences):
#    pass


# To Dev
#class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
#    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass