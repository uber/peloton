import pytest

from client import Client
from peloton_client.pbgen.peloton.api.job import job_pb2
from peloton_client.pbgen.peloton.api.query import query_pb2 as query
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton

from conftest import NUM_JOBS_PER_STATE

# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.jobquery]


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
    salt = create_jobs[0]
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]['RUNNING']

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
    assert len(resp.results) == NUM_JOBS_PER_STATE
    # test descending order, the last created active job should show up first
    assert resp.results[0].name == running_jobs[NUM_JOBS_PER_STATE -
                                                1].get_info().config.name


"""
test job query API to query completed jobs sorted by
creation time in desc order with pagination
"""


def test__query_completed_jobs(create_jobs):
    salt = create_jobs[0]
    respoolID = create_jobs[1]
    failed_jobs = create_jobs[2]['FAILED']

    # name is structured as "TestJob-<6 letter salt>-<count>"
    # we will use <salt> to restrict the query scope.
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

    # We should see NUM_JOBS_PER_STATE SUCCEEDED
    # and NUM_JOBS_PER_STATE FAILED jobs
    assert len(resp.results) == 2 * NUM_JOBS_PER_STATE

    # test descending order, the job that completed last should show up first
    # in this case it should be the last job in the failed jobs list

    assert resp.results[0].name == failed_jobs[NUM_JOBS_PER_STATE -
                                               1].get_info().config.name

# test pagination


def test__query_pagination(create_jobs):

    salt = create_jobs[0]
    respoolID = create_jobs[1]

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
    running_jobs = create_jobs[2]['RUNNING']

    spec_by_name = job_pb2.QuerySpec(
        name=running_jobs[0].get_info().config.name,
    )
    resp = query_by_spec(spec=spec_by_name)
    assert len(resp.results) == NUM_JOBS_PER_STATE

# test job query by owner


def test__query_job_by_owner(create_jobs):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]['RUNNING']

    spec_by_owner = job_pb2.QuerySpec(
        owner=running_jobs[0].get_info().config.owningTeam,
    )
    resp = query_by_spec(respoolID, spec_by_owner)
    # We should find NUM_JOBS_PER_STATE number of jobs for each of the
    # three states (RUNNING, FAILED, SUCCEEDED) for this owner
    assert len(resp.results) == 3 * NUM_JOBS_PER_STATE

# test job query by keyword


def test__query_job_by_keyword(create_jobs):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]['RUNNING']

    spec_by_keyword = job_pb2.QuerySpec(
        keywords=[running_jobs[0].get_info().config.name],
    )
    resp = query_by_spec(respoolID, spec_by_keyword)
    assert len(resp.results) == NUM_JOBS_PER_STATE

# test job query by owner and name


def test__query_job_by_owner_by_name(create_jobs):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]['RUNNING']

    spec_by_owner_name = job_pb2.QuerySpec(
        owner=running_jobs[0].get_info().config.owningTeam,
        name=running_jobs[0].get_info().config.name,
    )
    resp = query_by_spec(respoolID, spec_by_owner_name)
    assert len(resp.results) == NUM_JOBS_PER_STATE

# test job query by name and label


def test__query_job_by_name_by_label(create_jobs):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]['RUNNING']

    # query by name and label
    spec_by_label = job_pb2.QuerySpec(
        name=running_jobs[0].get_info().config.name,
        labels=[
            peloton.Label(key="testKey0", value="testVal0"),
        ]
    )
    resp = query_by_spec(respoolID, spec_by_label)
    assert len(resp.results) == NUM_JOBS_PER_STATE

# test job query with bad query spec. Expect empty results


def test__query_job_negative(create_jobs):
    respoolID = create_jobs[1]
    # query by name and label
    spec_by_name = job_pb2.QuerySpec(
        name='deadbeef',
    )
    resp = query_by_spec(respoolID, spec_by_name)
    assert len(resp.results) == 0
