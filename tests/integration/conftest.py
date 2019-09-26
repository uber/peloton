import logging
import os
import pytest
import time
import sys

import tools.minicluster.docker_client as docker_client
from tools.minicluster.utils import default_config as mc_config
import tools.minicluster.minicluster as minicluster
from host import (
    start_maintenance,
    complete_maintenance,
    wait_for_host_state,
)
from job import Job
from job import query_jobs as batch_query_jobs
from job import kill_jobs as batch_kill_jobs
from stateless_job import StatelessJob
from stateless_job import query_jobs as stateless_query_jobs
from m3.client import M3
from m3.emitter import BatchedEmitter
from peloton_client.pbgen.peloton.api.v0.host import host_pb2
from peloton_client.pbgen.peloton.api.v0.job import job_pb2
from conf_util import (
    TERMINAL_JOB_STATES,
    ACTIVE_JOB_STATES,
    MESOS_MASTER,
    MESOS_AGENTS,
)
import conf_util as util

log = logging.getLogger(__name__)


class TestMetrics(object):
    def __init__(self):
        self.failed = 0
        self.passed = 0
        self.duration = 0.0

    def increment_passed(self, duration):
        self.passed += 1
        self.duration += duration

    def increment_failed(self, duration):
        self.failed += 1
        self.duration += duration


collect_metrics = TestMetrics()


#
# Module scoped setup / teardown across test suites.
#
@pytest.fixture(scope="module", autouse=True)
def setup_cluster(request):
    tests_failed_before_module = request.session.testsfailed
    try:
        cluster = setup_minicluster()
    except Exception as e:
        log.error(e)
        sys.exit(1)

    def teardown_cluster():
        dump_logs = False
        if (request.session.testsfailed - tests_failed_before_module) > 0:
            dump_logs = True

        teardown_minicluster(cluster, dump_logs)

    request.addfinalizer(teardown_cluster)


@pytest.fixture(autouse=True)
def run_around_tests():
    # before each test
    yield
    # after each test
    cleanup_batch_jobs()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    global collect_metrics
    outcome = yield
    rep = outcome.get_result()
    if rep.outcome == "passed" and rep.when == "call":
        collect_metrics.increment_passed(rep.duration)
    if rep.outcome == "failed" and rep.when == "call":
        collect_metrics.increment_failed(rep.duration)

    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)

    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


class Container(object):
    def __init__(self, names, is_mesos_master=False):
        self._cluster = minicluster.default_cluster
        self._cli = docker_client.default_client
        self._names = names
        self._is_mesos_master = is_mesos_master

    def start(self):
        for name in self._names:
            self._cli.start(name)
            log.info("%s started", name)

        if self._is_mesos_master:
            self._cluster.wait_for_mesos_master_leader()

    def stop(self):
        for name in self._names:
            self._cli.stop(name, timeout=0)
            log.info("%s stopped", name)

    def restart(self):
        for name in self._names:
            self._cli.restart(name, timeout=0)
            log.info("%s restarted", name)

        if self._is_mesos_master:
            self._cluster.wait_for_mesos_master_leader()


def get_container(container_name):
    return Container(container_name)


def setup_minicluster(enable_k8s=False, use_host_pool=False,
                      isolate_cluster=False):
    """
    setup minicluster
    """
    log.info("setup cluster")
    cluster = minicluster.Minicluster(mc_config(), enable_peloton=True,
                                      enable_k8s=enable_k8s,
                                      use_host_pool=use_host_pool)
    if os.getenv("CLUSTER", ""):
        log.info("cluster mode")
        return cluster
    log.info("local minicluster mode")

    if isolate_cluster:
        ns = cluster.isolate()
        log.info("local minicluster isolated: " + ns)

    cluster.setup()
    return cluster


def teardown_minicluster(cluster, dump_logs=False):
    """
    teardown minicluster
    """
    log.info("\nteardown cluster")
    if os.getenv("CLUSTER", ""):
        log.info("cluster mode, no teardown actions")
        return
    elif os.getenv("NO_TEARDOWN", ""):
        log.info("skip teardown")
        return
    elif cluster is None:
        log.info("no cluster to tear down")
        return

    log.info("tearing down")

    # dump logs only if tests have failed in the current module
    if dump_logs and not os.getenv("NO_LOG_DUMPS"):
        # stop containers so that log stream will not block
        cluster.teardown(stop=True)

        try:
            # TODO (varung): enable PE and mesos-master logs if needed
            cli = cluster.cli
            for c in ("peloton-jobmgr0", "peloton-resmgr0"):
                for l in cli.logs(c, stream=True):
                    # remove newline character when logging
                    log.info(l.rstrip())
        except Exception as e:
            log.info(e)

    cluster.teardown()


def cleanup_batch_jobs():
    """
    stop all batch jobs from minicluster
    """
    jobs = batch_query_jobs()
    batch_kill_jobs(jobs)


@pytest.fixture()
def mesos_master():
    return Container(MESOS_MASTER, is_mesos_master=True)


@pytest.fixture()
def mesos_agent():
    # TODO: We need to pick up the count dynamically.
    return Container(MESOS_AGENTS)


@pytest.fixture()
def placement_engines():
    return Container(util.PLACEMENT_ENGINES)


@pytest.fixture()
def jobmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(util.JOB_MGRS)


@pytest.fixture()
def resmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(util.RES_MGRS)


@pytest.fixture()
def hostmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(util.HOST_MGRS)


@pytest.fixture()
def aurorabridge():
    # TODO: We need to pick up the count dynamically.
    return Container(util.AURORA_BRIDGE)


@pytest.fixture
def stateless_job(request):
    job = StatelessJob()
    if util.minicluster_type() == "k8s":
        job = StatelessJob(
            job_file="test_stateless_job_spec_k8s.yaml",
        )

    # teardown
    def kill_stateless_job():
        print("\nstopping stateless job")
        job.stop()

    request.addfinalizer(kill_stateless_job)

    return job


@pytest.fixture
def host_affinity_job(request):
    job = Job(
        job_file="test_job_host_affinity_constraint.yaml",
    )

    # Kill job
    def kill_host_affinity_job():
        print("\nstopping host affinity job")
        job.stop()

    request.addfinalizer(kill_host_affinity_job)
    return job


# For unit tests of update/restart running with in_place, it would
# be tested with both in_place feature enabled and disabled
@pytest.fixture(params=[True, False])
def in_place(request):
    return request.param


@pytest.fixture
def maintenance(request):
    draining_hosts = []
    client = [None]  # Use list to store a reference to the client object

    def update_client(new_client):
        client[0] = new_client

    def start(hosts):
        resp = start_maintenance(hosts, client=client[0])
        if not resp:
            log.error("Start maintenance failed:" + resp)
            return resp
        draining_hosts.extend(hosts)
        return resp

    def stop(hosts):
        resp = complete_maintenance(hosts, client=client[0])
        if not resp:
            log.error("Complete maintenance failed:" + resp)
            return resp

        # The mesos-agent containers needs to be started explicitly as they would
        # have been stopped when the agents transition to DOWN
        Container(hosts).start()
        del draining_hosts[:]

        return resp

    def clean_up():
        # kill stateless jobs. This is needed since host draining
        # is done in SLA aware manner for stateless jobs.
        for j in stateless_query_jobs(client=client[0]):
            j.stop()
        if not draining_hosts:
            return
        for h in draining_hosts:
            wait_for_host_state(h, host_pb2.HOST_STATE_DOWN)
        stop(draining_hosts)

    request.addfinalizer(clean_up)

    response = dict()
    response["start"] = start
    response["stop"] = stop
    response["update_client"] = update_client
    return response


"""
Setup fixture for getting a dict of job objects per state
"""


@pytest.fixture
def jobs_by_state(request):
    return util.create_job_config_by_state(_num_jobs_per_state=1)


"""
Setup/Cleanup fixture that starts a set of RUNNING, SUCCEEDED and
FAILED jobs scoped per module. This is to give each module a set
of active and completed jobs to test on.

Returns:
    common salt identifier, respoolID and dict of created jobs
"""


@pytest.fixture(scope="module")
def create_jobs(request):
    jobs_by_state = util.create_job_config_by_state()
    salt = jobs_by_state[0]
    jobs_dict = jobs_by_state[1]
    log.info("Create jobs")
    respoolID = None

    for state in TERMINAL_JOB_STATES:
        jobs = jobs_dict[state]
        for job in jobs:
            job.create()
            if state == "FAILED":
                job.wait_for_state(
                    goal_state="FAILED", failed_state="SUCCEEDED"
                )
            else:
                job.wait_for_state(goal_state=state)
            if respoolID is None:
                respoolID = job.get_config().respoolID

    def stop_jobs():
        log.info("Stop jobs")
        for state in TERMINAL_JOB_STATES:
            jobs = jobs_dict[state]
            for job in jobs:
                state = job_pb2.JobState.Name(job.get_runtime().state)
                if state in ACTIVE_JOB_STATES:
                    job.stop()
                    job.wait_for_state(goal_state="KILLED")

    request.addfinalizer(stop_jobs)

    # Job Query accuracy depends on lucene index being up to date
    # lucene index refresh time is 10 seconds. Sleep for 12 sec.
    time.sleep(12)
    return salt, respoolID, jobs_dict


"""
Setup/Cleanup fixture for tasks query integ-tests.
Within fixture parameter, a list of tuples,
such as [(task_state, count)], is passed to give each test case
a varied number of tasks to test on.

Returns:
    The job id of the job created.

"""


@pytest.fixture
def task_test_fixture(request):
    # task_states is a list of tuples, e.g. [('SUCCEEDED', 2)].
    task_states = request.param

    assert task_states is not None
    if len(task_states) > 1:
        mixed_task_states = True
    else:
        mixed_task_states = False
    test_config = util.generate_job_config(
        file_name="test_task.yaml", task_states=task_states
    )
    # Create job with customized tasks.
    job = Job(job_config=test_config)
    job.create()
    log.info("Job for task query is created: %s", job.job_id)

    # Determine terminating state.
    job_state = task_states[0][0] if not mixed_task_states else "FAILED"
    if job_state == "FAILED":
        job.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")
    else:
        job.wait_for_state(goal_state=job_state)

    def stop_job():
        state = job_pb2.JobState.Name(job.get_runtime().state)
        if state in ACTIVE_JOB_STATES:
            job.stop()
            job.wait_for_state(goal_state="KILLED")

    request.addfinalizer(stop_job)

    return job.job_id


"""
Setup/cleanup fixture that replaces a regular Mesos agent with
another one that has "peloton/exclusive" attribute. Cleanup does
the exact opposite.
"""


@pytest.fixture
def exclusive_host(request):
    cluster = minicluster.default_cluster

    def clean_up():
        cluster.set_mesos_agent_nonexclusive(0)
        cluster.wait_for_all_agents_to_register()

    cluster.set_mesos_agent_exclusive(0, "exclusive-test-label")
    cluster.wait_for_all_agents_to_register()

    request.addfinalizer(clean_up)
