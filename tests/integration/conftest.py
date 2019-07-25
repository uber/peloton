import logging
import os
import pytest
import time
import grpc
import requests

from docker import Client
from tools.minicluster.main import setup, teardown, config as mc_config
from tools.minicluster.minicluster import run_mesos_agent, teardown_mesos_agent
from job import Job
from job import query_jobs as batch_query_jobs
from job import kill_jobs as batch_kill_jobs
from host import start_maintenance, complete_maintenance, wait_for_host_state
from stateless_job import StatelessJob
from stateless_job import query_jobs as stateless_query_jobs
from stateless_job import delete_jobs as stateless_delete_jobs
from m3.client import M3
from m3.emitter import BatchedEmitter
from peloton_client.pbgen.peloton.api.v0.job import job_pb2
from peloton_client.pbgen.peloton.api.v0.host import host_pb2
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
    setup_minicluster()

    def teardown_cluster():
        dump_logs = False
        if (request.session.testsfailed - tests_failed_before_module) > 0:
            dump_logs = True

        teardown_minicluster(dump_logs)

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


def pytest_sessionfinish(session, exitstatus):
    emitter = BatchedEmitter()
    m3 = M3(
        application_identifier="peloton",
        emitter=emitter,
        environment="production",
        default_tags={"result": "watchdog", "cluster": os.getenv("CLUSTER")},
    )
    if collect_metrics.failed > 0:
        m3.gauge("watchdog_result", 1)
    else:
        m3.gauge("watchdog_result", 0)
    m3.gauge("total_tests", collect_metrics.failed + collect_metrics.passed)
    m3.gauge("failed_tests", collect_metrics.failed)
    m3.gauge("passed_tests", collect_metrics.passed)
    m3.gauge("duration_tests", collect_metrics.duration)


class Container(object):
    def __init__(self, names):
        self._cli = Client(base_url="unix://var/run/docker.sock")
        self._names = names

    def start(self):
        for name in self._names:
            self._cli.start(name)
            log.info("%s started", name)

        if self._names[0] in MESOS_MASTER:
            wait_for_mesos_master_leader()

    def stop(self):
        for name in self._names:
            self._cli.stop(name, timeout=0)
            log.info("%s stopped", name)

    def restart(self):
        for name in self._names:
            self._cli.restart(name, timeout=0)
            log.info("%s restarted", name)

        if self._names[0] in MESOS_MASTER:
            wait_for_mesos_master_leader()


def wait_for_mesos_master_leader(
    url="http://127.0.0.1:5050/state.json", timeout_secs=20
):
    """
    util method to wait for mesos master leader elected
    """

    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            resp = requests.get(url)
            if resp.status_code != 200:
                time.sleep(2)
                continue
            return
        except Exception:
            pass

    assert False, "timed out waiting for mesos master leader"


def setup_minicluster():
    """
    setup minicluster
    """
    log.info("setup cluster")
    if os.getenv("CLUSTER", ""):
        log.info("cluster mode")
    else:
        log.info("local minicluster mode")
        setup(enable_peloton=True)
        time.sleep(5)


def teardown_minicluster(dump_logs=False):
    """
    teardown minicluster
    """
    log.info("\nteardown cluster")
    if os.getenv("CLUSTER", ""):
        log.info("cluster mode, no teardown actions")
    elif os.getenv("NO_TEARDOWN", ""):
        log.info("skip teardown")
    else:
        log.info("tearing down")

        # dump logs only if tests have failed in the current module
        if dump_logs:
            # stop containers so that log stream will not block
            teardown(stop=True)

            try:
                # TODO (varung): enable PE and mesos-master logs if needed
                cli = Client(base_url="unix://var/run/docker.sock")
                for c in ("peloton-jobmgr0",
                          "peloton-resmgr0",
                          "peloton-hostmgr0",
                          "peloton-aurorabridge0"):
                    for l in cli.logs(c, stream=True):
                        # remove newline character when logging
                        log.info(l.rstrip())
            except Exception as e:
                log.info(e)

        teardown()


def cleanup_batch_jobs():
    """
    stop all batch jobs from minicluster
    """
    jobs = batch_query_jobs()
    batch_kill_jobs(jobs)


def cleanup_stateless_jobs(timeout_secs=10):
    """
    delete all service jobs from minicluster
    """
    jobs = stateless_query_jobs()

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
            time.sleep(2)
        except grpc.RpcError as e:
            # Catch "not-found" error here because QueryJobs endpoint does
            # two db queries in sequence: "QueryJobs" and "GetUpdate".
            # However, when we delete a job, updates are deleted first,
            # there is a slight chance QueryJobs will fail to query the
            # update, returning "not-found" error.
            if e.code() == grpc.StatusCode.NOT_FOUND:
                time.sleep(2)
                continue


@pytest.fixture()
def mesos_master():
    return Container(MESOS_MASTER)


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
def long_running_job(request):
    job = Job(job_file="long_running_job.yaml")

    # teardown
    def kill_long_running_job():
        print("\nstopping long running job")
        job.stop()

    request.addfinalizer(kill_long_running_job)

    return job


@pytest.fixture
def stateless_job(request):
    job = StatelessJob()

    # teardown
    def kill_stateless_job():
        print("\nstopping stateless job")
        job.stop()

    request.addfinalizer(kill_stateless_job)

    return job


@pytest.fixture
def host_affinity_job(request):
    job = Job(job_file="test_job_host_affinity_constraint.yaml")

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

    def start(hosts):
        resp = start_maintenance(hosts)
        if not resp:
            log.error("Start maintenance failed:" + resp)
            return resp

        draining_hosts.extend(hosts)
        return resp

    def stop(hosts):
        resp = complete_maintenance(hosts)
        if not resp:
            log.error("Complete maintenance failed:" + resp)
            return resp

        for h in hosts:
            draining_hosts.remove(h)
            # The mesos-agent containers needs to be started explicitly as they would
            # have been stopped when the agents transition to DOWN
            Container([h]).start()

        return resp

    def clean_up():
        if not draining_hosts:
            return
        for h in draining_hosts:
            wait_for_host_state(h, host_pb2.HOST_STATE_DOWN)
        stop(draining_hosts)

    request.addfinalizer(clean_up)

    response = dict()
    response["start"] = start
    response["stop"] = stop
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
    def clean_up():
        teardown_mesos_agent(mc_config, 0, is_exclusive=True)
        run_mesos_agent(mc_config, 0, 0)
        time.sleep(5)

    # Remove agent #0 and instead create exclusive agent #0
    teardown_mesos_agent(mc_config, 0)
    run_mesos_agent(
        mc_config,
        0,
        3,
        is_exclusive=True,
        exclusive_label_value="exclusive-test-label",
    )
    time.sleep(5)
    request.addfinalizer(clean_up)
