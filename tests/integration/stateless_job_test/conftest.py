import pytest
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
    cleanup_stateless_jobs,
    wait_for_all_agents_to_register,
)


@pytest.fixture(scope="module", autouse=True)
def bootstrap_cluster(request):
    tests_failed_before_module = request.session.testsfailed
    enable_k8s = request.node.get_marker('k8s') is not None
    setup_minicluster(enable_k8s=enable_k8s)

    yield

    dump_logs = False
    if (request.session.testsfailed - tests_failed_before_module) > 0:
        dump_logs = True

    teardown_minicluster(dump_logs)


@pytest.fixture(scope="module", autouse=True)
def setup_cluster(request):
    """
    override parent module fixture
    """
    pass


@pytest.fixture(autouse=True)
def run_around_tests():
    wait_for_all_agents_to_register()
    yield
    # after each test
    cleanup_stateless_jobs()
