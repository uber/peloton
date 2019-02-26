import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    check_response_ok,
    start_job_update,
    wait_for_killed,
)

# TODO(kevinxu): Add back default tag until we figure out why _delete_jobs()
# is failing at some times.
# pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]
pytestmark = [pytest.mark.aurorabridge]


def test__start_job_update__get_jobs__get_job_summary(client):
    '''
    test__start_job_update__get_jobs__get_job_summary tests creating
    three jobs and verifies get_jobs and get_job_summary return the
    correct job.

    steps:
    1. start job update for test/dc/labrat, wait for rolled forward
    2. start job update for test/dc/labrat0, wait for rolled forward
    3. start job update for test2/dc2/labrat2, wait for rolled forward
    4. verify get_job_summary() returns current result for "test" role
    5. verify get_jobs() returns correct result for "test" role
    '''

    # verify no jobs returns based on results from both
    # get_job_summary and get_jobs
    resp = client.get_job_summary('')
    check_response_ok(resp)
    result = resp.result
    assert result.jobSummaryResult is not None
    assert result.jobSummaryResult.summaries is None or \
        0 == len(result.jobSummaryResult.summaries)

    resp = client.get_jobs('')
    check_response_ok(resp)
    result = resp.result
    assert result.getJobsResult is not None
    assert result.getJobsResult.configs is None or \
        0 == len(result.getJobsResult.configs)

    # create two jobs
    test_dc_labrat_key = start_job_update(
        client,
        'test_dc_labrat.yaml',
        'start job update test/dc/labrat',
    ).job
    test_dc_labrat_0_key = start_job_update(
        client,
        'test_dc_labrat0.yaml',
        'start job update test/dc/labrat0',
    ).job
    start_job_update(
        client,
        'test2_dc2_labrat2.yaml',
        'start job update test2/dc2/labrat2',
    )

    # tests get_job_summary
    resp = client.get_job_summary(test_dc_labrat_key.role)
    check_response_ok(resp)
    result = resp.result
    assert result.jobSummaryResult is not None
    summaries = list(result.jobSummaryResult.summaries)
    assert 2 == len(summaries)

    for s in summaries:
        assert 3 == s.stats.activeTaskCount
        assert test_dc_labrat_key == s.job.key or test_dc_labrat_0_key == s.job.key
        assert 3 == s.job.instanceCount
        assert test_dc_labrat_key == s.job.taskConfig.job or \
            test_dc_labrat_0_key == s.job.taskConfig.job

    # tests get_jobs
    resp = client.get_jobs(test_dc_labrat_key.role)
    check_response_ok(resp)
    result = resp.result
    assert result.getJobsResult is not None
    configs = list(result.getJobsResult.configs)
    assert 2 == len(configs)

    for c in configs:
        assert test_dc_labrat_key == c.key or test_dc_labrat_0_key == c.key
        assert 3 == c.instanceCount
        assert test_dc_labrat_key == c.taskConfig.job or \
            test_dc_labrat_0_key == c.taskConfig.job


def test__start_job_update__get_tasks_without_configs(client):
    '''
    test__start_job_update__get_tasks_without_configs tests creating multiple
    jobs and verifies get_tasks_without_configs return the tasks from the
    correct job based on query.

    steps:
    1. start job update for test/dc/labrat, wait for rolled forward
    2. start job update for test/dc/labrat0, wait for rolled forward
    3. start job update for test/dc0/labrat1, wait for rolled forward
    4. start job update for test/dc/labrat1, wait for rolled forward
    5. start job update for test/dc2/labrat2, wait for rolled forward
    6. kill tasks from test/dc/labrat1, wait for tasks to be killed
    7. query tasks using job keys ["test/dc/labrat", "test/dc/labrat0",
       "test2/dc2/labrat2"], verify correct tasks are returned
    8. query tasks using role "test", env "dc", name "labrat", verify only
       tasks from test/dc/labrat are returned
    9. query tasks using role "test", env "dc", verify only tasks from
       test/dc/labrat, test/dc/labrat0 and test/dc/labrat1 are returned
    10. query tasks using role "test", verify only tasks from
        test/dc/labrat, test/dc/labrat0, test/dc0/labrat1, test/dc/labrat1
        are returned
    11. query tasks using role "test" and schedule status "RUNNING", verify
        only tasks from test/dc/labrat, test/dc/labrat0, test/dc0/labrat1
        are returned.
    '''

    # verify no jobs returns
    resp = client.get_tasks_without_configs(api.TaskQuery())
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    assert result.scheduleStatusResult.tasks is None or \
        0 == len(result.scheduleStatusResult.tasks)

    # create two jobs
    test_dc_labrat_key = start_job_update(
        client,
        'test_dc_labrat.yaml',
        'start job update test/dc/labrat',
    ).job
    test_dc_labrat_0_key = start_job_update(
        client,
        'test_dc_labrat0.yaml',
        'start job update test/dc/labrat0',
    ).job
    test_dc_0_labrat_1_key = start_job_update(
        client,
        'test_dc0_labrat1.yaml',
        'start job update test/dc0/labrat1',
    ).job
    test_dc_labrat_1_key = start_job_update(
        client,
        'test_dc_labrat1.yaml',
        'start job update test/dc/labrat1',
    ).job
    test2_dc2_labrat2_key = start_job_update(
        client,
        'test2_dc2_labrat2.yaml',
        'start job update test2/dc2/labrat2',
    ).job

    # stop test/dc/labrat1
    resp = client.kill_tasks(
        test_dc_labrat_1_key,
        set([0, 1, 2]),
        'killing all tasks test/dc/labrat1',
    )
    check_response_ok(resp)
    wait_for_killed(client, test_dc_labrat_1_key)

    # tests get_tasks_without_configs with job keys
    resp = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys=set([test_dc_labrat_key, test_dc_labrat_0_key, test2_dc2_labrat2_key]),
    ))
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    tasks = result.scheduleStatusResult.tasks
    assert (3 * 3) == len(tasks)

    for t in tasks:
        assert api.ScheduleStatus.RUNNING == t.status
        assert api.ScheduleStatus.RUNNING == t.taskEvents[-1].status
        assert test_dc_labrat_key == t.assignedTask.task.job or \
            test_dc_labrat_0_key == t.assignedTask.task.job or \
            test2_dc2_labrat2_key == t.assignedTask.task.job

    # tests get_tasks_without_configs with role, env, name
    resp = client.get_tasks_without_configs(api.TaskQuery(
        role=test_dc_labrat_key.role,
        environment=test_dc_labrat_key.environment,
        jobName=test_dc_labrat_key.name,
    ))
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    tasks = result.scheduleStatusResult.tasks
    assert 3 == len(tasks)

    for t in tasks:
        assert test_dc_labrat_key == t.assignedTask.task.job

    # tests get_tasks_without_configs with role, env
    resp = client.get_tasks_without_configs(api.TaskQuery(
        role=test_dc_labrat_key.role,
        environment=test_dc_labrat_key.environment,
    ))
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    tasks = result.scheduleStatusResult.tasks
    assert (3 * 3) == len(tasks)
    for t in tasks:
        assert test_dc_labrat_key == t.assignedTask.task.job or \
            test_dc_labrat_0_key == t.assignedTask.task.job or \
            test_dc_labrat_1_key == t.assignedTask.task.job

    # tests get_tasks_without_configs with role
    resp = client.get_tasks_without_configs(api.TaskQuery(
        role=test_dc_labrat_key.role,
    ))
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    tasks = result.scheduleStatusResult.tasks
    assert (4 * 3) == len(tasks)

    for t in tasks:
        assert test_dc_labrat_key == t.assignedTask.task.job or \
            test_dc_labrat_0_key == t.assignedTask.task.job or \
            test_dc_labrat_1_key == t.assignedTask.task.job or \
            test_dc_0_labrat_1_key == t.assignedTask.task.job

    # tests get_tasks_without_configs with role and statuses
    resp = client.get_tasks_without_configs(api.TaskQuery(
        role=test_dc_labrat_key.role,
        statuses=set([api.ScheduleStatus.RUNNING])
    ))
    check_response_ok(resp)
    result = resp.result
    assert result.scheduleStatusResult is not None
    tasks = result.scheduleStatusResult.tasks
    assert (3 * 3) == len(tasks)

    for t in tasks:
        assert test_dc_labrat_key == t.assignedTask.task.job or \
            test_dc_labrat_0_key == t.assignedTask.task.job or \
            test_dc_0_labrat_1_key == t.assignedTask.task.job
