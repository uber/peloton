import logging
import os
import pytest
import random
import string
import time

from docker import Client
from tools.pcluster.pcluster import setup, teardown
from job import Job
from stateless_job import StatelessJob
from m3.client import M3
from m3.emitter import BatchedEmitter
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api.v0.job import job_pb2
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from util import load_test_config

from collections import defaultdict

log = logging.getLogger(__name__)

NUM_JOBS_PER_STATE = 1
STATES = ['SUCCEEDED', 'RUNNING', 'FAILED']
ACTIVE_STATES = ['PENDING', 'RUNNING', 'INITIALIZED']


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
    log.info('setup cluster')
    if os.getenv('CLUSTER', ''):
        log.info('cluster mode')
    else:
        log.info('local pcluster mode')
        setup(enable_peloton=True)
        time.sleep(5)

    def teardown_cluster():
        log.info('\nteardown cluster')
        if os.getenv('CLUSTER', ''):
            log.info('cluster mode, no teardown actions')
        elif os.getenv('NO_TEARDOWN', ''):
            log.info('skip teardown')
        else:
            log.info('teardown, writing logs')
            try:
                cli = Client(base_url='unix://var/run/docker.sock')
                log.info(cli.logs('peloton-jobmgr0'))
                log.info(cli.logs('peloton-jobmgr1'))
            except Exception as e:
                log.info(e)
            teardown()

    request.addfinalizer(teardown_cluster)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    global collect_metrics
    outcome = yield
    rep = outcome.get_result()
    if rep.outcome == "passed" and rep.when == "call":
        collect_metrics.increment_passed(rep.duration)
    if rep.outcome == "failed" and rep.when == "call":
        collect_metrics.increment_failed(rep.duration)


def pytest_sessionfinish(session, exitstatus):
    emitter = BatchedEmitter()
    m3 = M3(application_identifier='peloton',
            emitter=emitter,
            environment='production',
            default_tags={
                'result': 'watchdog', 'cluster': os.getenv('CLUSTER')},
            )
    if collect_metrics.failed > 0:
        m3.gauge('watchdog_result', 1)
    else:
        m3.gauge('watchdog_result', 0)
    m3.gauge('total_tests', collect_metrics.failed + collect_metrics.passed)
    m3.gauge('failed_tests', collect_metrics.failed)
    m3.gauge('passed_tests', collect_metrics.passed)
    m3.gauge('duration_tests', collect_metrics.duration)


class Container(object):
    def __init__(self, names):
        self._cli = Client(base_url='unix://var/run/docker.sock')
        self._names = names

    def start(self):
        for name in self._names:
            self._cli.start(name)
            log.info('%s started', name)

    def stop(self):
        for name in self._names:
            self._cli.stop(name, timeout=0)
            log.info('%s stopped', name)

    def restart(self):
        for name in self._names:
            self._cli.restart(name, timeout=0)
            log.info('%s restarted', name)


@pytest.fixture()
def mesos_master():
    return Container(['peloton-mesos-master'])


@pytest.fixture()
def jobmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(['peloton-jobmgr0', 'peloton-jobmgr1'])


@pytest.fixture()
def resmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(['peloton-resmgr0', 'peloton-resmgr1'])


@pytest.fixture()
def hostmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(['peloton-hostmgr0', 'peloton-hostmgr1'])


@pytest.fixture()
def mesos_agent():
    # TODO: We need to pick up the count dynamically.
    return Container(['peloton-mesos-agent0', 'peloton-mesos-agent1',
                      'peloton-mesos-agent2'])


@pytest.fixture
def long_running_job(request):
    job = Job(job_file='long_running_job.yaml')

    # teardown
    def kill_long_running_job():
        print "\nstopping long running job"
        job.stop()

    request.addfinalizer(kill_long_running_job)

    return job


@pytest.fixture
def stateless_job(request):
    job = Job(job_file='test_stateless_job.yaml')

    # teardown
    def kill_stateless_job():
        print "\nstopping stateless job"
        job.stop()

    request.addfinalizer(kill_stateless_job)

    return job


@pytest.fixture
def large_stateless_job(request):
    job = Job(job_file='test_stateless_job_large.yaml')

    # teardown
    def kill_large_stateless_job():
        print "\nstopping stateless job"
        job.stop()

    request.addfinalizer(kill_large_stateless_job)

    return job


@pytest.fixture
def stateless_job_v1alpha(request):
    job = StatelessJob()

    # teardown
    def kill_stateless_job():
        print "\nstopping stateless job"
        job.stop()

    request.addfinalizer(kill_stateless_job)

    return job


@pytest.fixture
def host_affinity_job(request):
    job = Job(job_file='test_job_host_affinity_constraint.yaml')

    # Kill job
    def kill_host_affinity_job():
        print "\nstopping host affinity job"
        job.stop()

    request.addfinalizer(kill_host_affinity_job)
    return job


# For unit tests running with test_job, it would be tested with both
# long_running_job and stateless_job
@pytest.fixture(params=[long_running_job, stateless_job_v1alpha])
def test_job(request):
    return request.param(request)


"""
Setup fixture for getting a dict of job objects per state
"""


@pytest.fixture
def jobs_by_state(request):
    return _jobs_by_state(_num_jobs_per_state=1)


"""
Setup/Cleanup fixture that starts a set of RUNNING, SUCCEEDED and
FAILED jobs scoped per module. This is to give each module a set
of active and completed jobs to test on.

Returns:
    common salt identifier, respoolID and dict of created jobs
"""


@pytest.fixture(scope="module")
def create_jobs(request):
    jobs_by_state = _jobs_by_state()
    salt = jobs_by_state[0]
    jobs_dict = jobs_by_state[1]
    log.info('Create jobs')
    respoolID = None

    for state in STATES:
        jobs = jobs_dict[state]
        for job in jobs:
            job.create()
            if state is 'FAILED':
                job.wait_for_state(goal_state='FAILED',
                                   failed_state='SUCCEEDED')
            else:
                job.wait_for_state(goal_state=state)
            if respoolID is None:
                respoolID = job.get_config().respoolID

    def stop_jobs():
        log.info('Stop jobs')
        for state in STATES:
            jobs = jobs_dict[state]
            for job in jobs:
                state = job_pb2.JobState.Name(job.get_runtime().state)
                if state in ACTIVE_STATES:
                    job.stop()
                    job.wait_for_state(goal_state='KILLED')

    request.addfinalizer(stop_jobs)

    # Job Query accuracy depends on lucene index being up to date
    # lucene index refresh time is 10 seconds. Sleep for 12 sec.
    time.sleep(12)
    return salt, respoolID, jobs_dict


"""
Load job config from file and modify its specific fields
we use this function to create jobs with potentially unique names
and predictable run times and states which we can then query in
a predictable manner (query by the same name/owner/state)

Args:
    job_file: Load base config from this file
    name: Job name string.
    owner: Job owner string.
    long_running: when set, job is supposed to run for 60 seconds
    simulate_failure: If set, job config is created to simulate task failure

Returns:
    job_pb2.JobConfig object is returned.

Raises:
    None
"""


def generate_job_config(job_file='test_job.yaml',
                        name='test', owner='compute',
                        long_running=False,
                        simulate_failure=False):
    job_config_dump = load_test_config(job_file)
    job_config = job_pb2.JobConfig()
    json_format.ParseDict(job_config_dump, job_config)

    command_string = "echo 'this is a test' &  sleep 1"

    if long_running:
        command_string += " & sleep 60"

    if simulate_failure:
        command_string += " & exit(2)"
    job_config.name = name
    job_config.owningTeam = owner
    task_cfg = task.TaskConfig(
        command=mesos.CommandInfo(
            shell=True,
            value=command_string,
        ),
    )
    job_config.defaultConfig.MergeFrom(task_cfg)
    return job_config


"""
Get a map of job objects by state and their common identifier salt

Args:
    _num_jobs_per_state: number of job objects per state.

Returns:
    dict of jobs list per state is returned
"""


def _jobs_by_state(_num_jobs_per_state=NUM_JOBS_PER_STATE):
    salt = ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(6))
    name = 'TestJob-' + salt
    owner = 'compute-' + salt
    jobs_by_state = defaultdict(list)

    # create three jobs per state with this owner and name
    for state in STATES:
        for i in xrange(_num_jobs_per_state):
            long_running = True if state is 'RUNNING' else False
            simulate_failure = True if state is 'FAILED' else False
            job = Job(job_config=generate_job_config(
                # job name will be in format: TestJob-<salt>-<inst-id>-<state>
                name=name + '-' + str(i) + '-' + state,
                owner=owner, long_running=long_running,
                simulate_failure=simulate_failure))
            jobs_by_state[state].append(job)
    return salt, jobs_by_state
