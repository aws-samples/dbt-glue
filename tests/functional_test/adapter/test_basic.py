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

# To test
#class TestDocsGenerate(BaseDocsGenerate):
#    pass


#class TestDocsGenReferences(BaseDocsGenReferences):
#    pass


# To Dev
class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            }
        }

    pass


#class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
#    pass