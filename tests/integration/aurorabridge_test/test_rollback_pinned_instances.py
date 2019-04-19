import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_auto_rolling_back,
    wait_for_rolled_back,
    wait_for_rolled_forward,
    wait_for_update_status,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge]


def test__auto_rollback_with_pinned_instances(client):
    """
    1. Create a job
    2. Start a bad update (version 2) targeting subset of instances,
    3. Wait for the instances to be auto-rolled back.
       Only the instances specified above should be affected.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        assert run_id == '1'
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == 'test_key_1':
                assert m.value == 'test_value_1'
            elif m.key == 'test_key_2':
                assert m.value == 'test_value_2'
            else:
                assert False, 'unexpected metadata %s' % m

    # start a bad update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
    pinned_req = get_job_update_request('test_dc_labrat_large_job_bad_config.yaml')
    pinned_req.settings.updateOnlyTheseInstances = set([api.Range(first=i, last=i)
                                                        for i in update_instances])
    pinned_req.settings.updateGroupSize = 5
    pinned_req.settings.maxFailedInstances = 3

    res = client.start_job_update(
        pinned_req,
        'start job update test/dc/labrat_large_job with pinned instances')
    wait_for_rolled_back(client, res.key)

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    # verify that the run-id of only the pinned instances has changed
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == '3'
        else:
            assert run_id == '1'

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == 'test_key_1':
                assert m.value == 'test_value_1'
            elif m.key == 'test_key_2':
                assert m.value == 'test_value_2'
            else:
                assert False, 'unexpected metadata %s for unaffected instances' % m


def test__abort_auto_rollback_with_pinned_instances_and_update(client):
    """
    1. Create a job
    2. Start a bad update (version 2) targeting subset of instances,
    3. Wait for the auto-rollback to kick-in.
    4. Once auto-rollback kicks in, abort the update.
    4. Start a new good update and wait for all instances to converge to that update.
    """
    # Create a job
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        assert run_id == '1'
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == 'test_key_1':
                assert m.value == 'test_value_1'
            elif m.key == 'test_key_2':
                assert m.value == 'test_value_2'
            else:
                assert False, 'unexpected metadata %s' % m

    # start a bad update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
    pinned_req = get_job_update_request('test_dc_labrat_large_job_bad_config.yaml')
    pinned_req.settings.updateOnlyTheseInstances = set([api.Range(first=i, last=i)
                                                        for i in update_instances])
    pinned_req.settings.maxFailedInstances = 4

    res = client.start_job_update(
        pinned_req,
        'start job update test/dc/labrat_large_job with pinned instances')
    # wait for auto-rollback to kick-in
    wait_for_auto_rolling_back(client, res.key, timeout_secs=150)

    # abort the update
    client.abort_job_update(res.key, 'abort update')
    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_BACK},
        api.JobUpdateStatus.ABORTED,
    )

    # start a new good update
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_new_config.yaml'),
        'start job update test/dc/labrat_large_job with a good config')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        assert len(t.assignedTask.task.metadata) == 1
        for m in t.assignedTask.task.metadata:
            if m.key == 'test_key_12':
                assert m.value == 'test_value_12'
            else:
                assert False, 'unexpected metadata %s' % m

        if t.assignedTask.instanceId in update_instances:
            # only a few of the pinned instances might have rolled back
            assert run_id == '3' or run_id == '4'
        else:
            assert run_id == '2'
