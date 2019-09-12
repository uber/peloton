import pytest
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
)
from tests.integration.job import Job


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


@pytest.fixture(autouse=True)
def run_around_tests(minicluster):
    minicluster.wait_for_all_agents_to_register()
    yield


@pytest.fixture
def peloton_client(minicluster):
    yield minicluster.peloton_client()


@pytest.fixture
def job_factory(peloton_client):
    def factory(*args, **kwargs):
        kwargs["client"] = peloton_client
        return Job(*args, **kwargs)
    return factory


@pytest.fixture
def long_running_job(request, peloton_client):
    job = Job(job_file="long_running_job.yaml", client=peloton_client)

    # teardown
    def kill_long_running_job():
        print("\nstopping long running job")
        job.stop()

    request.addfinalizer(kill_long_running_job)

    return job
