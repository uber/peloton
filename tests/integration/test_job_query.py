import pytest
import string
import random
import time
import logging

from client import Client
from job import Job
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api.job import job_pb2
from peloton_client.pbgen.peloton.api.query import query_pb2 as query
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.task import task_pb2 as task
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from util import load_test_config

log = logging.getLogger(__name__)

# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.jobquery]

ACTIVE_JOB_COUNT = 3
COMPLETED_JOB_COUNT = 2
TOTAL_JOB_COUNT = ACTIVE_JOB_COUNT + COMPLETED_JOB_COUNT

"""
Setup/Cleanup fixture for job query test

Returns:
    List of created jobs and respoolID is returned as a tuple

"""


@pytest.fixture(scope="module")
def create_jobs(request):
    # create jobs
    log.info('\nCreate jobs')
    jobs = []
    respoolID = None
    salt = ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(6))
    # Have unique job name and owner per run so that the scope of
    # job query can be restricted and we can test predictable behavior
    name = 'TestJobForJobQuery-' + salt
    owner = 'compute-' + salt

    # Create two short running jobs, and simulate failure on one.
    job = Job(job_config=generate_job_config(
        name=name + '-0', owner=owner, simulate_failure=True))
    job.create()
    job.wait_for_state(goal_state='FAILED', failed_state='SUCCEEDED')
    jobs.append(job)
    job = Job(job_config=generate_job_config(
        name=name + '-1', owner=owner))
    job.create()
    job.wait_for_state()
    jobs.append(job)

    # create three jobs with this owner and name
    for i in xrange(ACTIVE_JOB_COUNT):
        job = Job(job_config=generate_job_config(
            name=name + '-' + str(i+COMPLETED_JOB_COUNT),
            owner=owner, long_running=True))
        job.create()
        job.wait_for_state(goal_state='RUNNING')
        jobs.append(job)
        if respoolID is None:
            respoolID = job.get_config().respoolID

    def stop_jobs():
        log.info('\nStop jobs')
        for job in jobs:
            state = job_pb2.JobState.Name(job.get_runtime().state)
            if state in ['PENDING', 'RUNNING', 'INITIALIZED']:
                job.stop()
                job.wait_for_state(goal_state='KILLED')

    request.addfinalizer(stop_jobs)

    # Job Query accuracy depends on lucene index being up to date
    # lucene index refresh time is 10 seconds. Sleep for 12 sec.
    time.sleep(12)
    return jobs, respoolID


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

    command_string = "echo 'lol' &  sleep 1"

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
Helper function to call Job Query API with the provided query spec.

Args:
    respool_id: peloton.ResourcePoolID to query within that respool
    spec: job_pb2.QuerySpec object that contains different query params

Returns:
    job_pb2.QueryResponse object is returned.

Raises:
    None
"""


def query_by_spec(respool_id=None, spec=None):
    client = Client()
    request = job_pb2.QueryRequest(
        respoolID=respool_id,
        spec=spec,
        summaryOnly=True,
    )
    resp = client.job_svc.Query(
        request,
        metadata=client.jobmgr_metadata,
        timeout=10,
    )
    return resp


# Tests begin:
# These tests are meant to test the queries that are most
# commonly used (mostly ones used by UI and CLI)


"""
test job query API to query active jobs sorted by
creation time in desc order with pagination
"""


def test__query_active_jobs(create_jobs):

    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    # name is structured as "TestJobForJobQuery-<6 letter salt>-<count>"
    # we will use <salt> to restrict the query scope.
    salt = jobs[0].get_info().config.name.split('-')[1]
    spec_active_jobs = job_pb2.QuerySpec(
        keywords=[salt],
        jobStates=[
            job_pb2.JobState.Value('RUNNING'),
            job_pb2.JobState.Value('PENDING'),
            job_pb2.JobState.Value('INITIALIZED'),
        ],
        pagination=query.PaginationSpec(
            offset=0,
            limit=500,
            maxLimit=1000,
            orderBy=[query.OrderBy(
                order=query.OrderBy.Order.Value('DESC'),
                property=query.PropertyPath(value='creation_time'),
            )],
        ),
    )
    resp = query_by_spec(respoolID, spec=spec_active_jobs)
    assert len(resp.results) == ACTIVE_JOB_COUNT
    # test descending order, the last created active job should show up first
    assert resp.results[0].name == jobs[TOTAL_JOB_COUNT -
                                        1].get_info().config.name


"""
test job query API to query completed jobs sorted by
creation time in desc order with pagination
"""


def test__query_completed_jobs(create_jobs):

    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    # name is structured as "TestJobForJobQuery-<6 letter salt>-<count>"
    # we will use <salt> to restrict the query scope.
    salt = jobs[0].get_info().config.name.split('-')[1]
    spec_completed_jobs = job_pb2.QuerySpec(
        keywords=[salt],
        jobStates=[
            job_pb2.JobState.Value('SUCCEEDED'),
            job_pb2.JobState.Value('KILLED'),
            job_pb2.JobState.Value('FAILED'),
        ],
        pagination=query.PaginationSpec(
            offset=0,
            limit=500,
            maxLimit=1000,
            orderBy=[query.OrderBy(
                order=query.OrderBy.Order.Value('DESC'),
                property=query.PropertyPath(value='completion_time'),
            )],
        ),
    )
    resp = query_by_spec(respoolID, spec=spec_completed_jobs)
    assert len(resp.results) == COMPLETED_JOB_COUNT
    # test descending order, the last completed job which is the 2nd job
    # in the jobs list, should show up first
    assert resp.results[0].name == jobs[COMPLETED_JOB_COUNT -
                                        1].get_info().config.name

# test pagination


def test__query_pagination(create_jobs):

    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    salt = jobs[0].get_info().config.name.split('-')[1]
    pagination = query.PaginationSpec(
        offset=0,
        limit=2,
        maxLimit=5,
    )
    spec_pagination = job_pb2.QuerySpec(
        keywords=[salt],
        pagination=pagination,
    )
    resp = query_by_spec(respoolID, spec=spec_pagination)
    assert len(resp.results) == 2

    pagination.maxLimit = 2
    spec_pagination = job_pb2.QuerySpec(
        keywords=[salt],
        pagination=pagination,
    )
    resp = query_by_spec(respoolID, spec=spec_pagination)
    assert len(resp.results) == 2

    pagination.offset = 1
    spec_pagination = job_pb2.QuerySpec(
        keywords=[salt],
        pagination=pagination,
    )
    resp = query_by_spec(respoolID, spec=spec_pagination)
    assert len(resp.results) == 1


# test job query by name (no respoolID)


def test__query_job_by_name(create_jobs):
    jobs = create_jobs[0]
    spec_by_name = job_pb2.QuerySpec(
        name=jobs[0].get_info().config.name,
    )
    resp = query_by_spec(spec=spec_by_name)
    assert len(resp.results) == 1

# test job query by owner


def test__query_job_by_owner(create_jobs):
    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    spec_by_owner = job_pb2.QuerySpec(
        owner=jobs[0].get_info().config.owningTeam,
    )
    resp = query_by_spec(respoolID, spec_by_owner)
    assert len(resp.results) == TOTAL_JOB_COUNT

# test job query by keyword


def test__query_job_by_keyword(create_jobs):
    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    spec_by_keyword = job_pb2.QuerySpec(
        keywords=[jobs[0].get_info().config.name],
    )
    resp = query_by_spec(respoolID, spec_by_keyword)
    assert len(resp.results) == 1

# test job query by owner and name


def test__query_job_by_owner_by_name(create_jobs):
    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    spec_by_owner_name = job_pb2.QuerySpec(
        owner=jobs[0].get_info().config.owningTeam,
        name=jobs[0].get_info().config.name,
    )
    resp = query_by_spec(respoolID, spec_by_owner_name)
    assert len(resp.results) == 1

# test job query by name and label


def test__query_job_by_name_by_label(create_jobs):
    jobs = create_jobs[0]
    respoolID = create_jobs[1]
    # query by name and label
    spec_by_label = job_pb2.QuerySpec(
        name=jobs[0].get_info().config.name,
        labels=[
            peloton.Label(key="testKey0", value="testVal0"),
        ]
    )
    resp = query_by_spec(respoolID, spec_by_label)
    assert len(resp.results) == 1

# test job query with bad query spec. Expect empty results


def test__query_job_negative(create_jobs):
    respoolID = create_jobs[1]
    # query by name and label
    spec_by_name = job_pb2.QuerySpec(
        name='deadbeef',
    )
    resp = query_by_spec(respoolID, spec_by_name)
    assert len(resp.results) == 0
