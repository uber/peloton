import pytest
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    start_job_update,
    wait_for_rolled_back,
    verify_events_sorted,
    verify_first_and_last_job_update_status,
    verify_task_config,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge,
              pytest.mark.random_order(disabled=True)]


@pytest.mark.skip(reason="pending host to task map fix at resmgr")
def test__simple_auto_rolled_back(client):
    """
    Create a job, then issue a bad config update and validate
    job is rolled back to previous version
    """
    start_job_update(
        client,
        'test_dc_labrat.yaml',
        'start job update test/dc/labrat')

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_bad_config.yaml'),
        'rollout bad config')
    wait_for_rolled_back(client, res.key)

    # validate job is rolled back to previous config
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))

    tasks = res.tasks
    assert len(tasks) == 3

    for t in tasks:
        for r in t.assignedTask.task.resources:
            if r.numCpus > 0:
                assert r.numCpus == 0.25
            elif r.ramMb > 0:
                assert r.ramMb == 128
            elif r.diskMb > 0:
                assert r.diskMb == 128
            else:
                assert False, 'unexpected resource {}'.format(r)


@pytest.mark.skip(reason="succeeded job is not deleted")
def test__job_create_manual_rollback(client):
    """
    Start a job update, and half-way to a manual rollback
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    job_update_key = res.key
    job_key = res.key.job

    # wait for few instances running
    time.sleep(5)

    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # rollback update
    client.rollback_job_update(job_update_key)
    wait_for_rolled_back(client, job_update_key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0


def test__simple_manual_rollback(client):
    """
    Start a job update which will create a job. Do another update on the job
    and half-way to a manual rollback
    """
    job_key = start_job_update(
        client,
        'test_dc_labrat_large_job.yaml',
        'start job update test/dc/labrat_large_job')

    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLED_FORWARD)
    verify_task_config(client, job_key, {
        "test_key_1": "test_value_1",
        "test_key_2": "test_value_2"})

    # Do update on previously created job
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_labels.yaml'),
        'start job update test/dc/labrat_large_job_diff_labels')
    job_update_key = res.key
    job_key = res.key.job

    # wait for few instances running
    time.sleep(5)

    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLING_FORWARD)
    verify_task_config(client, job_key, {
        "test_key_1": "test_value_1",
        "test_key_2": "test_value_2",
        "test_key_11": "test_value_11",
        "test_key_22": "test_value_22"})

    # rollback update
    client.rollback_job_update(job_update_key)
    wait_for_rolled_back(client, job_update_key)

    # verify events are sorted ascending, and last update event is ROLLED_BACK
    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLED_BACK)
    verify_task_config(client, job_key, {
        "test_key_1": "test_value_1",
        "test_key_2": "test_value_2"})  # rolled back to previous task config
