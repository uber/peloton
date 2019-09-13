import pytest
import logging


from tests.integration.client import Client
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.v0.query import query_pb2 as query
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton

from tests.integration.conf_util import (
    TASK_STATES,
    DEFAUILT_TASKS_COUNT,
    MESOS_AGENTS,
)

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.taskquery,
    pytest.mark.random_order(disabled=True),
]

log = logging.getLogger(__name__)

###################################
# Common setup and helper functions
###################################
SORT_BY_FILEDS = ["state", "creation_time"]
ORDER = ["DESC", "ASC"]

default_pagination = query.PaginationSpec(
    offset=0,
    limit=500,
    maxLimit=1000,
    orderBy=[
        query.OrderBy(
            order=query.OrderBy.Order.Value("DESC"),
            property=query.PropertyPath(value="creation_time"),
        )
    ],
)


def to_spec(task_state):
    """ Creates a task.TaskState message. """
    assert task_state in TASK_STATES
    spec = task.TaskState.Value(task_state)
    return spec


def query_request(
    job_id, task_state=None, pagination=None, names=None, hosts=None
):
    """ Constructs a task.QueryRequest object for task query api. """
    job_id_spec = peloton.JobID(value=job_id)
    task_states = [task_state] if task_state else []
    pagination = pagination if pagination else default_pagination
    task_query_spec = task.QuerySpec(
        taskStates=task_states, pagination=pagination, names=names, hosts=hosts
    )

    request = task.QueryRequest(jobId=job_id_spec, spec=task_query_spec)
    return request


def query_response(request, client):
    """
    Invokes task query API, and returns a task.QueryRequest object
    from api response.
    """
    client = Client()
    task_query_response = client.task_svc.Query(
        request, metadata=client.jobmgr_metadata, timeout=10
    )
    return task_query_response


def update_orderby(sort_by, order="DESC"):
    """ Customize query.PaginationSpec object by field to sort on and ordering."""
    assert sort_by in SORT_BY_FILEDS
    assert order in ORDER

    orderby = [
        query.OrderBy(
            order=query.OrderBy.Order.Value(order),
            property=query.PropertyPath(value=sort_by),
        )
    ]
    del default_pagination.orderBy[:]
    default_pagination.orderBy.extend(orderby)
    return default_pagination


def is_sorted(sort_results, sort_by_field, sort_order):
    """ Checks if sort_results is sorted based on the field and order specified.

    Args:
        sort_results: task list of TaskQuery response
        sort_by_field: string type. `sort_by_field` can have value of
            'state' or 'creation_time'.
        sort_order: string type. `sort_order` can have value of
            'DESC' or 'ASC'.

    Returns:
        True if the given list is sorted by the specified field in the specified order.
        Otherwise, returns False.
    """
    assert sort_by_field in SORT_BY_FILEDS
    assert sort_order in ORDER

    def compare(x, y):
        if sort_order == "DESC":
            return x >= y
        else:
            return x <= y

    return all(
        compare(
            _get_field_value(sort_results[i], sort_by_field),
            _get_field_value(sort_results[i + 1], sort_by_field),
        )
        for i in xrange(len(sort_results) - 1)
    )


"""
Extracts the value from TaskQuery response.

Arg:
    query_response: TaskQuery response.
    sort_by_field: string type. `sort_by_field` can have value of
            'state' or 'creation_time'.

Returns:
    If TaskQuery response is sorted by 'state', the Task_State of the
    response is returned; if it's sorted by 'creation_time', the
    value of task `startTime` is returned.
"""


def _get_field_value(query_response, sort_by):
    assert isinstance(query_response, task.TaskInfo)
    assert sort_by in SORT_BY_FILEDS

    if sort_by == "state":
        state = task.TaskState.Name(query_response.runtime.state)
        return TASK_STATES.index(state)
    return query_response.runtime.startTime


##########################################################
# Tests begin.
##########################################################


"""
Queries for succeeded tasks in a `SUCCEEDED` job, which contains 3 tasks; expects
all tasks are returned from api.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 3)])], indirect=True
)
def test__query_succeeded_tasks(task_test_fixture, peloton_client):
    succeeded_state = to_spec("SUCCEEDED")
    request = query_request(task_test_fixture, succeeded_state)
    succeeded_tasks = query_response(request, peloton_client)

    assert len(succeeded_tasks.records) == DEFAUILT_TASKS_COUNT


"""
Queries for running tasks in a running job, whose all 3 tasks are of state `RUNNING`;
expects that all tasks are returned and are running.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("RUNNING", DEFAUILT_TASKS_COUNT)])], indirect=True
)
def test__query_running_tasks(task_test_fixture, peloton_client):
    running_state = to_spec("RUNNING")
    request = query_request(task_test_fixture, running_state)
    running_tasks = query_response(request, peloton_client)

    assert len(running_tasks.records) == DEFAUILT_TASKS_COUNT


"""
Queries for failed tasks in a failed job, whose all 3 tasks are of state `FAILED`;
expects all tasks are returned and are failed.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("FAILED", DEFAUILT_TASKS_COUNT)])], indirect=True
)
def test__query_failed_tasks(task_test_fixture, peloton_client):
    failed_state = to_spec("FAILED")
    request = query_request(task_test_fixture, failed_state)
    failed_tasks = query_response(request, peloton_client)

    assert len(failed_tasks.records) == DEFAUILT_TASKS_COUNT


"""
Tests the number of tasks under different states; expects that their add up to the
total tasks number.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 1), ("FAILED", 2)])], indirect=True
)
def test__jobs_consist_of_mixed_task_states(task_test_fixture, peloton_client):
    req_succeeded = query_request(task_test_fixture, to_spec("SUCCEEDED"))
    succeeded = query_response(req_succeeded, peloton_client)
    count_succeeded = len(succeeded.records)

    req_failed = query_request(task_test_fixture, to_spec("FAILED"))
    failed = query_response(req_failed, peloton_client)
    count_failed = len(failed.records)
    assert count_succeeded == 1
    assert count_failed == 2


"""
TaskQuery by name.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 3), ("FAILED", 2)])], indirect=True
)
def test__task_query_by_name(task_test_fixture, peloton_client):
    req_by_name_1 = query_request(task_test_fixture, names=["task-1"])
    resp = query_response(req_by_name_1, peloton_client)
    assert len(resp.records) == 1
    for resp_task in resp.records:
        assert resp_task.config.name == "task-1"

    req_by_name_2 = query_request(
        task_test_fixture, names=["task-2", "task-3"]
    )
    response = query_response(req_by_name_2, peloton_client)
    assert len(response.records) == 2
    for resp_task in response.records:
        assert (
            resp_task.config.name == "task-2"
            or resp_task.config.name == "task-3"
        )


"""
TaskQuery by host.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 3), ("FAILED", 2)])], indirect=True
)
def test__task_query_by_host(task_test_fixture, peloton_client):
    req_by_hosts = query_request(task_test_fixture, hosts=MESOS_AGENTS)
    resp = query_response(req_by_hosts, peloton_client)
    assert len(resp.records) == 5
    for resp_task in resp.records:
        assert resp_task.runtime.host in MESOS_AGENTS


"""
Tests pagination's sort by state, and sort by create time, whereas these two support
sorting on task page.
"""


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 3)])], indirect=True
)
def test__sort_by_creation_time(task_test_fixture, peloton_client):
    # test desc order.
    sort_by_creation = update_orderby("creation_time")
    request = query_request(task_test_fixture, pagination=sort_by_creation)
    sort_response = query_response(request, peloton_client)

    assert is_sorted(sort_response.records, "creation_time", "DESC")

    # test asc order.
    asc_sort_by_creation = update_orderby("creation_time", "ASC")
    request = query_request(task_test_fixture, pagination=asc_sort_by_creation)
    sort_response = query_response(request, peloton_client)

    assert is_sorted(sort_response.records, "creation_time", "ASC")


@pytest.mark.parametrize(
    "task_test_fixture", [([("SUCCEEDED", 3)])], indirect=True
)
def test__sort_by_state(task_test_fixture, peloton_client):
    # test desc order.
    sort_by_state = update_orderby("state")
    request = query_request(task_test_fixture, pagination=sort_by_state)
    sort_response = query_response(request, peloton_client)

    assert is_sorted(sort_response.records, "state", "DESC")

    # test asc order.
    asc_sort_by_state = update_orderby("state", "ASC")
    request = query_request(task_test_fixture, pagination=asc_sort_by_state)
    sort_response = query_response(request, peloton_client)

    assert is_sorted(sort_response.records, "state", "ASC")
