import grpc
import os
import pytest
import time

from tests.integration.client import Client
from tests.integration.job import Job
from tests.integration.stateless_job import (
    StatelessJob,
    delete_jobs as stateless_delete_jobs,
    query_jobs as stateless_query_jobs,
)
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
)
import tests.integration.conf_util as util
from tests.integration.host import (
    ensure_host_pool,
    cleanup_other_host_pools,
)


@pytest.fixture(scope="module", autouse=True)
def minicluster(request):
    tests_failed_before_module = request.session.testsfailed

    enable_k8s = util.minicluster_type()
    should_isolate = os.getenv("MINICLUSTER_ISOLATE") == "1"
    cluster = setup_minicluster(
        enable_k8s=enable_k8s,
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
    use_host_pool = util.use_host_pool()
    if use_host_pool:
        ensure_host_pool(util.HOSTPOOL_STATELESS, 3, client=peloton_client)
    yield
    # after each test
    cleanup_stateless_jobs(client=peloton_client)
    if use_host_pool:
        cleanup_other_host_pools(
            [util.HOSTPOOL_STATELESS],
            client=peloton_client,
        )


@pytest.fixture
def stateless_job(request, peloton_client):
    job = StatelessJob(
        client=peloton_client,
    )
    if util.minicluster_type() == "k8s":
        job = StatelessJob(
            job_file="test_stateless_job_spec_k8s.yaml",
            client=peloton_client,
        )

    # teardown
    def kill_stateless_job():
        print("\nstopping stateless job")
        job.stop()

    request.addfinalizer(kill_stateless_job)

    return job


@pytest.fixture
def host_affinity_job(request, peloton_client):
    job = Job(
        job_file="test_job_host_affinity_constraint.yaml",
        client=peloton_client,
    )

    # Kill job
    def kill_host_affinity_job():
        print("\nstopping host affinity job")
        job.stop()

    request.addfinalizer(kill_host_affinity_job)
    return job


def cleanup_stateless_jobs(timeout_secs=10, client=None):
    """
    delete all service jobs from minicluster
    """
    client = client or Client()
    jobs = stateless_query_jobs(client=client)

    # opportunistic delete for jobs, if not deleted within
    # timeout period, it will get cleanup in next test run.
    stateless_delete_jobs(jobs)

    # Wait for job deletion to complete.
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            jobs = stateless_query_jobs()
            if len(jobs) == 0:
                return
            time.sleep(1)
        except grpc.RpcError as e:
            # Catch "not-found" error here because QueryJobs endpoint does
            # two db queries in sequence: "QueryJobs" and "GetUpdate".
            # However, when we delete a job, updates are deleted first,
            # there is a slight chance QueryJobs will fail to query the
            # update, returning "not-found" error.
            if e.code() == grpc.StatusCode.NOT_FOUND:
                time.sleep(1)
                continue
