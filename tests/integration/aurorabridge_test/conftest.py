import pytest

from tests.integration.aurorabridge_test.client import Client
from tests.integration.aurorabridge_test.util import delete_jobs
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
)


@pytest.fixture(scope="session", autouse=True)
def minicluster(request):
    tests_failed_before_module = request.session.testsfailed
    cluster = setup_minicluster()

    yield cluster

    dump_logs = False
    if (request.session.testsfailed - tests_failed_before_module) > 0:
        dump_logs = True

    teardown_minicluster(cluster, dump_logs)


@pytest.fixture(scope="module", autouse=True)
def setup_cluster(request):
    """
    override parent module fixture
    """
    pass


@pytest.fixture
def client(minicluster):
    minicluster.wait_for_all_agents_to_register()
    client = Client()

    yield client

    # Delete all jobs
    delete_jobs()
