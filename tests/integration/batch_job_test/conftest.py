import pytest
import os

from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
)
import tests.integration.conf_util as util
from tests.integration.host import (
    ensure_host_pool,
    cleanup_other_host_pools,
)
from tests.integration.job import Job


@pytest.fixture(scope="session", autouse=True)
def minicluster(request):
    tests_failed_before_module = request.session.testsfailed

    should_isolate = os.getenv("MINICLUSTER_ISOLATE") == "1"
    cluster = setup_minicluster(
        use_host_pool=util.use_host_pool(),
        isolate_cluster=should_isolate,
    )

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
def peloton_client(minicluster):
    yield minicluster.peloton_client()


@pytest.fixture(autouse=True)
def run_around_tests(minicluster, peloton_client):
    minicluster.wait_for_all_agents_to_register()
    if not util.use_host_pool():
        yield
        return

    ensure_host_pool(util.HOSTPOOL_BATCH_RESERVED, 2, client=peloton_client)
    ensure_host_pool(util.HOSTPOOL_SHARED, 1, client=peloton_client)
    yield
    cleanup_other_host_pools(
        [util.HOSTPOOL_BATCH_RESERVED, util.HOSTPOOL_SHARED],
        client=peloton_client,
    )


@pytest.fixture
def long_running_job(request, peloton_client):
    job = Job(job_file="long_running_job.yaml", client=peloton_client)

    # teardown
    def kill_long_running_job():
        print("\nstopping long running job")
        job.stop()

    request.addfinalizer(kill_long_running_job)

    return job
