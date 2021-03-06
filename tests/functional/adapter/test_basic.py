import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
    pass


class TestSingularTestsGlue(BaseSingularTests):
    pass

# To test
#class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
#    pass


class TestEmptyGlue(BaseEmpty):
    pass


class TestEphemeralGlue(BaseEphemeral):
    pass


class TestIncrementalGlue(BaseIncremental):
    pass


class TestGenericTestsGlue(BaseGenericTests):
    pass


# To Dev
#class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
#    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass