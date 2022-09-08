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


#class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
    # all tests within this test has the same schema
#    @pytest.fixture(scope="class")
#    def unique_schema(request, prefix) -> str:
#        return "dbt_functional_test_01"

#    pass


#class TestSingularTestsGlue(BaseSingularTests):
#    pass

# To test
#class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
#    pass


#class TestEmptyGlue(BaseEmpty):
#    pass


#class TestEphemeralGlue(BaseEphemeral):
    # all tests within this test has the same schema
#    @pytest.fixture(scope="class")
#    def unique_schema(request, prefix) -> str:
#        return "dbt_functional_test_01"

#    pass


class TestIncrementalGlue(BaseIncremental):
    pass


#class TestGenericTestsGlue(BaseGenericTests):
#    pass


#class TestDocsGenerate(BaseDocsGenerate):
#    pass


#class TestDocsGenReferences(BaseDocsGenReferences):
#    pass


# To Dev
#class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
#    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass