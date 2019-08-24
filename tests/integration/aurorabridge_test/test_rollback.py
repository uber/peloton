import pytest
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    start_job_update,
    wait_for_auto_rolling_back,
    wait_for_rolled_back,
    wait_for_rolled_forward,
    wait_for_update_status,
    wait_for_failed,
    verify_events_sorted,
    verify_first_and_last_job_update_status,
    verify_task_config,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__simple_auto_rolled_back(client):
    """
    Create a job, then issue a bad config update and validate
    job is rolled back to previous version
    """
    start_job_update(
        client, "test_dc_labrat.yaml", "start job update test/dc/labrat"
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_bad_config.yaml"),
        "rollout bad config",
    )
    wait_for_rolled_back(client, res.key)

    # validate job is rolled back to previous config
    res = client.get_tasks_without_configs(
        api.TaskQuery(
            jobKeys={res.key.job}, statuses={api.ScheduleStatus.RUNNING}
        )
    )

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
            elif r.namedPort:
                assert r.namedPort in ["grpc", "http"]
            else:
                assert False, "unexpected resource {}".format(r)


def test__job_create_manual_rollback(client):
    """
    Start a job update, and half-way to a manual rollback
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    job_update_key = res.key
    job_key = res.key.job

    # wait for few instances running
    time.sleep(5)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # rollback update
    client.rollback_job_update(job_update_key)
    wait_for_rolled_back(client, job_update_key)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0


def test__simple_manual_rollback(client):
    """
    Start a job update which will create a job. Do another update on the job
    and half-way to a manual rollback
    """
    job_key = start_job_update(
        client,
        "test_dc_labrat_large_job.yaml",
        "start job update test/dc/labrat_large_job",
    )

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLED_FORWARD,
    )
    verify_task_config(
        client,
        job_key,
        {"test_key_1": "test_value_1", "test_key_2": "test_value_2"},
    )

    # Do update on previously created job
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml"),
        "start job update test/dc/labrat_large_job_diff_labels",
    )
    job_update_key = res.key
    job_key = res.key.job

    # wait for few instances running
    time.sleep(5)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLING_FORWARD,
    )
    verify_task_config(
        client,
        job_key,
        {
            "test_key_1": "test_value_1",
            "test_key_2": "test_value_2",
            "test_key_11": "test_value_11",
            "test_key_22": "test_value_22",
        },
    )

    # rollback update
    client.rollback_job_update(job_update_key)
    wait_for_rolled_back(client, job_update_key)

    # verify events are sorted ascending, and last update event is ROLLED_BACK
    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    verify_events_sorted(res.detailsList[0].updateEvents)
    verify_events_sorted(res.detailsList[0].instanceEvents)
    verify_first_and_last_job_update_status(
        res.detailsList[0].updateEvents,
        api.JobUpdateStatus.ROLLING_FORWARD,
        api.JobUpdateStatus.ROLLED_BACK,
    )
    verify_task_config(
        client,
        job_key,
        {"test_key_1": "test_value_1", "test_key_2": "test_value_2"},
    )  # rolled back to previous task config


def test__job_create_fail_manual_rollback(client):
    """
    Start a failed job update, while half-way in the update,
    trigger a manual rollback
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_bad_config.yaml"),
        "start job update test/dc/labrat_large_job (failed)",
    )
    job_update_key = res.key
    job_key = res.key.job

    # wait for the first instance starting
    time.sleep(5)
    wait_for_failed(client, job_key, instances=[0])

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # rollback update
    client.rollback_job_update(job_update_key)
    wait_for_rolled_back(client, job_update_key)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # validate no tasks are running
    res = client.get_tasks_without_configs(
        api.TaskQuery(
            jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING}
        )
    )

    tasks = res.tasks
    assert len(tasks) == 0


def test__abort_auto_rollback_and_update(client):
    """
    1. Create a job
    2. Start a bad update, wait for auto-rollback to kick-in
    3. Once auto-rollback starts, abort an update.
    4. Do a new good update and all the instances should converge to the new config.
    """
    start_job_update(
        client,
        "test_dc_labrat_large_job.yaml",
        "start job update test/dc/labrat_large_job",
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_bad_config.yaml"),
        "rollout bad config",
    )

    # wait for auto-rollback to kick-in
    wait_for_auto_rolling_back(client, res.key)

    client.abort_job_update(res.key, "abort update")
    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_BACK},
        api.JobUpdateStatus.ABORTED,
    )

    new_config = get_job_update_request(
        "test_dc_labrat_large_job_new_config.yaml"
    )
    res = client.start_job_update(new_config, "rollout good config")
    # Sleep for a while so that update gets triggered.
    time.sleep(5)
    wait_for_rolled_forward(client, res.key)

    res = client.get_tasks_without_configs(
        api.TaskQuery(
            jobKeys={res.key.job}, statuses={api.ScheduleStatus.RUNNING}
        )
    )
    assert len(res.tasks) == 10

    for t in res.tasks:
        assert len(t.assignedTask.task.metadata) == 1
        assert (
            list(t.assignedTask.task.metadata)[0].key
            == list(new_config.taskConfig.metadata)[0].key
        )
        assert (
            list(t.assignedTask.task.metadata)[0].value
            == list(new_config.taskConfig.metadata)[0].value
        )

        assert t.ancestorId
