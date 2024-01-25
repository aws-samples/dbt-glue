import pytest

from tests.functional.adapter.test_basic import TestSimpleMaterializationsGlue


@pytest.fixture(scope="class")
def use_arrow():
    return True


class TestSimpleMaterializationsArrowGlue(TestSimpleMaterializationsGlue):
    pass
