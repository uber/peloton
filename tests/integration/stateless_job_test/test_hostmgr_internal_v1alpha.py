import pytest

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.k8s,
    pytest.mark.hostmgr_internal,
]


def test__acquire_leases():
    pass
