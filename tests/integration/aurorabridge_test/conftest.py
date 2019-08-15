import pytest

from tests.integration.aurorabridge_test.client import Client
from tests.integration.aurorabridge_test.util import delete_jobs
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
    wait_for_all_agents_to_register,
)


@pytest.fixture(scope="session", autouse=True)
def bootstrap_cluster(request):
    tests_failed_before_module = request.session.testsfailed
    setup_minicluster()

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


@pytest.fixture
def client():
    wait_for_all_agents_to_register()
    client = Client()

    yield client

    # Delete all jobs
    delete_jobs()
