import pytest
from datetime import datetime, timedelta

from peloton_client.pbgen.peloton.api.v0.job import job_pb2
from peloton_client.pbgen.peloton.api.v0.query import query_pb2 as query
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.google.protobuf import timestamp_pb2

from tests.integration.conf_util import NUM_JOBS_PER_STATE

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.jobquery,
    pytest.mark.random_order(disabled=True),
]


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


def query_by_spec(peloton_client, respool_id=None, spec=None):
    client = peloton_client
    request = job_pb2.QueryRequest(
        respoolID=respool_id, spec=spec, summaryOnly=True
    )
    resp = client.job_svc.Query(
        request, metadata=client.jobmgr_metadata, timeout=10
    )
    return resp


# Tests begin:
# These tests are meant to test the queries that are most
# commonly used (mostly ones used by UI and CLI)


"""
test job query API to query active jobs sorted by
creation time in desc order with pagination
"""


def test__query_active_jobs(create_jobs, peloton_client):
    salt = create_jobs[0]
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]["RUNNING"]

    spec_active_jobs = job_pb2.QuerySpec(
        keywords=[salt],
        jobStates=[
            job_pb2.JobState.Value("RUNNING"),
            job_pb2.JobState.Value("PENDING"),
            job_pb2.JobState.Value("INITIALIZED"),
        ],
        pagination=query.PaginationSpec(
            offset=0,
            limit=500,
            maxLimit=1000,
            orderBy=[
                query.OrderBy(
                    order=query.OrderBy.Order.Value("DESC"),
                    property=query.PropertyPath(value="creation_time"),
                )
            ],
        ),
    )
    resp = query_by_spec(peloton_client, respoolID, spec=spec_active_jobs)
    assert len(resp.results) == NUM_JOBS_PER_STATE
    # test descending order, the last created active job should show up first
    assert (
        resp.results[0].name
        == running_jobs[NUM_JOBS_PER_STATE - 1].get_info().config.name
    )


"""
test job query API to query completed jobs sorted by
creation time in desc order with pagination
"""


def test__query_completed_jobs(create_jobs, peloton_client):
    salt = create_jobs[0]
    respoolID = create_jobs[1]
    failed_jobs = create_jobs[2]["FAILED"]

    # name is structured as "TestJob-<6 letter salt>-<count>"
    # we will use <salt> to restrict the query scope.
    spec_completed_jobs = job_pb2.QuerySpec(
        keywords=[salt],
        jobStates=[
            job_pb2.JobState.Value("SUCCEEDED"),
            job_pb2.JobState.Value("KILLED"),
            job_pb2.JobState.Value("FAILED"),
        ],
        pagination=query.PaginationSpec(
            offset=0,
            limit=500,
            maxLimit=1000,
            orderBy=[
                query.OrderBy(
                    order=query.OrderBy.Order.Value("DESC"),
                    property=query.PropertyPath(value="completion_time"),
                )
            ],
        ),
    )
    resp = query_by_spec(peloton_client, respoolID, spec=spec_completed_jobs)

    # We should see NUM_JOBS_PER_STATE SUCCEEDED
    # and NUM_JOBS_PER_STATE FAILED jobs
    assert len(resp.results) == 2 * NUM_JOBS_PER_STATE

    # test descending order, the job that completed last should show up first
    # in this case it should be the last job in the failed jobs list

    assert (
        resp.results[0].name
        == failed_jobs[NUM_JOBS_PER_STATE - 1].get_info().config.name
    )


# test pagination
def test__query_pagination(create_jobs, peloton_client):
    salt = create_jobs[0]
    respoolID = create_jobs[1]

    pagination = query.PaginationSpec(offset=0, limit=2, maxLimit=5)
    spec_pagination = job_pb2.QuerySpec(keywords=[salt], pagination=pagination)
    resp = query_by_spec(peloton_client, respoolID, spec=spec_pagination)
    assert len(resp.results) == 2

    pagination.maxLimit = 2
    spec_pagination = job_pb2.QuerySpec(keywords=[salt], pagination=pagination)
    resp = query_by_spec(peloton_client, respoolID, spec=spec_pagination)
    assert len(resp.results) == 2

    pagination.offset = 1
    spec_pagination = job_pb2.QuerySpec(keywords=[salt], pagination=pagination)
    resp = query_by_spec(peloton_client, respoolID, spec=spec_pagination)
    assert len(resp.results) == 1


# test job query by name (no respoolID)
def test__query_job_by_name(create_jobs, peloton_client):
    running_jobs = create_jobs[2]["RUNNING"]

    spec_by_name = job_pb2.QuerySpec(
        name=running_jobs[0].get_info().config.name
    )
    resp = query_by_spec(peloton_client, spec=spec_by_name)
    assert len(resp.results) == NUM_JOBS_PER_STATE


# test job query by owner
def test__query_job_by_owner(create_jobs, peloton_client):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]["RUNNING"]

    spec_by_owner = job_pb2.QuerySpec(
        owner=running_jobs[0].get_info().config.owningTeam
    )
    resp = query_by_spec(peloton_client, respoolID, spec_by_owner)
    # We should find NUM_JOBS_PER_STATE number of jobs for each of the
    # three states (RUNNING, FAILED, SUCCEEDED) for this owner
    assert len(resp.results) == 3 * NUM_JOBS_PER_STATE


# test job query by keyword
def test__query_job_by_keyword(create_jobs, peloton_client):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]["RUNNING"]

    spec_by_keyword = job_pb2.QuerySpec(
        keywords=[running_jobs[0].get_info().config.name]
    )
    resp = query_by_spec(peloton_client, respoolID, spec_by_keyword)
    assert len(resp.results) == NUM_JOBS_PER_STATE


# test job query by owner and name
def test__query_job_by_owner_by_name(create_jobs, peloton_client):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]["RUNNING"]

    spec_by_owner_name = job_pb2.QuerySpec(
        owner=running_jobs[0].get_info().config.owningTeam,
        name=running_jobs[0].get_info().config.name,
    )
    resp = query_by_spec(peloton_client, respoolID, spec_by_owner_name)
    assert len(resp.results) == NUM_JOBS_PER_STATE


# test job query by name and label
def test__query_job_by_name_by_label(create_jobs, peloton_client):
    respoolID = create_jobs[1]
    running_jobs = create_jobs[2]["RUNNING"]

    # query by name and label
    spec_by_label = job_pb2.QuerySpec(
        name=running_jobs[0].get_info().config.name,
        labels=[peloton.Label(key="testKey0", value="testVal0")],
    )
    resp = query_by_spec(peloton_client, respoolID, spec_by_label)
    assert len(resp.results) == NUM_JOBS_PER_STATE


# test job query with bad query spec. Expect empty results
def test__query_job_negative(create_jobs, peloton_client):
    respoolID = create_jobs[1]
    # query by name and label
    spec_by_name = job_pb2.QuerySpec(name="deadbeef")
    resp = query_by_spec(peloton_client, respoolID, spec_by_name)
    assert len(resp.results) == 0


def test__query_time_range(create_jobs, peloton_client):
    salt = create_jobs[0]
    respoolID = create_jobs[1]

    min_time = timestamp_pb2.Timestamp()
    max_time = timestamp_pb2.Timestamp()
    dt = datetime.today() - timedelta(days=1)
    min_time.FromDatetime(dt)
    max_time.GetCurrentTime()
    time_range = peloton.TimeRange(min=min_time, max=max_time)
    spec = job_pb2.QuerySpec(keywords=[salt], completionTimeRange=time_range)
    resp = query_by_spec(peloton_client, respoolID, spec=spec)
    # This is a query with keyword for jobs completed
    # over last one day. This should yield
    # 2 * NUM_JOBS_PER_STATE jobs for SUCCEEDED and
    # FAILED states, both of which will have completion time
    assert len(resp.results) == 2 * NUM_JOBS_PER_STATE

    spec = job_pb2.QuerySpec(keywords=[salt], creationTimeRange=time_range)
    resp = query_by_spec(peloton_client, respoolID, spec=spec)
    # This is a query with keyword for jobs completed
    # over last one day. This should yield
    # 3 * NUM_JOBS_PER_STATE jobs for RUNNING, SUCCEEDED and
    # FAILED states
    assert len(resp.results) == 3 * NUM_JOBS_PER_STATE

    # use min_time as max and max_time as min
    # This should result in error in response
    bad_time_range = peloton.TimeRange(max=min_time, min=max_time)
    spec = job_pb2.QuerySpec(keywords=[salt], creationTimeRange=bad_time_range)
    resp = query_by_spec(peloton_client, respoolID, spec=spec)
    assert resp.HasField("error")
