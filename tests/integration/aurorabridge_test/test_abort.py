import pytest
import time
import random
from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    start_job_update,
    get_job_update_request,
    wait_for_update_status,
    get_update_status,
    wait_for_rolled_forward,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]

# TODO (varung):
# - Create an update, let it auto rollback and abort it.
# - Create an update, do manual rollback with pulsed and abort it.
# - Create an update, do manual rollback, pause it and then abort it.
# - Abort an update, after 1st instance is RUNNING.


def test__deploy_on_aborted_update(client):
    """
    Deploy an update, and abort half-way. Then re-deploy
    same update. Updated instances should not restart again.
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )

    # Few instances start
    time.sleep(5)

    client.abort_job_update(res.key, "abort update")
    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ABORTED,
    )

    # Not all instances were created
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={res.key.job})
    )
    assert len(res.tasks) < 10

    # deploy same update, should impact remaining instances
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml"),
        "start job update test/dc/labrat_large_job_diff_executor",
    )
    wait_for_rolled_forward(client, res.key)

    # All instances are created
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={res.key.job})
    )
    assert len(res.tasks) == 10

    # No instances should have ancestor id, thereby validating
    # instances created in previous update are not restarted/redeployed
    for task in res.tasks:
        assert task.ancestorId is None


def test__rolling_forward_abort(client):
    """
    Create an update and then abort it
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat.yaml"), "create job"
    )
    key = res.key

    # Sleep for sometime to let Peloton transition workflow state
    # from INITIALIZED -> ROLLING_FORWARD or let it be as-is
    time.sleep(5)

    client.abort_job_update(key, "abort update")
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ABORTED,
    )


def test__roll_forward_paused_update_abort(client):
    """
    Create an update, pause it and then abort it
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat.yaml"), "create job"
    )
    key = res.key

    # Sleep for sometime to let Peloton transition workflow state
    # from INITIALIZED -> ROLLING_FORWARD or let it be as-is
    time.sleep(5)

    client.pause_job_update(key, "pause update")
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ROLL_FORWARD_PAUSED,
    )

    client.abort_job_update(key, "abort update")
    wait_for_update_status(
        client,
        key,
        {
            api.JobUpdateStatus.ROLLING_FORWARD,
            api.JobUpdateStatus.ROLL_FORWARD_PAUSED,
        },
        api.JobUpdateStatus.ABORTED,
    )


def test__pulsed_update_abort(client):
    """
    Create a pulse update, and then abort it
    """
    req = get_job_update_request("test_dc_labrat_pulsed.yaml")
    res = client.start_job_update(
        req, "start pulsed job update test/dc/labrat"
    )
    assert (
        get_update_status(client, res.key)
        == api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE
    )
    key = res.key

    client.abort_job_update(key, "abort update")
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE},
        api.JobUpdateStatus.ABORTED,
    )


def test__manual_rollback_abort(client):
    """
    - Create Job
    - Start an update
    - Perform manual rollback on rolling_forward update
    - Abort rolling_back update
    - Stateless job will actually have two updates, but bridge will dedupe
      last two updates (as manual rollback was done)
    - Validate that task config for each instance
    """
    # Create a job and wait for it to complete
    start_job_update(
        client,
        "test_dc_labrat_large_job.yaml",
        "start job update test/dc/labrat_large_job",
    )

    # Do update on previously created job
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml"),
        "start job update test/dc/labrat_large_job_diff_labels",
    )
    job_update_key = res.key
    job_key = res.key.job

    # wait for few instances running
    time.sleep(10)

    # rollback update
    client.rollback_job_update(job_update_key)

    # wait for sometime to trigger manual rollback
    time.sleep(5)

    # abort the manual rollback
    client.abort_job_update(job_update_key, "abort update")

    # 2 updates must be present for this job
    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )
    assert len(res.detailsList) == 2

    # first event is rolling forward
    assert (
        res.detailsList[0].updateEvents[0].status
        == api.JobUpdateStatus.ROLLING_FORWARD
    )

    # second last element is rolling back, after manual rollback is triggered
    assert (
        res.detailsList[0].updateEvents[-2].status
        == api.JobUpdateStatus.ROLLING_BACK
    )

    # most recent event is aborted, once manual rollback is aborted
    assert (
        res.detailsList[0].updateEvents[-1].status
        == api.JobUpdateStatus.ABORTED
    )

    # wait for all tasks to be running after invoking abort
    count = 0
    while count < 6:
        res = client.get_tasks_without_configs(
            api.TaskQuery(jobKeys={job_key}, statuses={
                          api.ScheduleStatus.RUNNING})
        )
        if len(res.tasks) == 10:
            break

        count = count + 1
        time.sleep(10)

    # run-id == 1: Job Create
    # run-id == 2: Job Update with diff labels
    # run-id == 3: Update rollback to previous version
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if run_id == "1" or run_id == "3":
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        elif run_id == "2":
            # Preference to update instances has changed.
            # Previously behavior, start to update instance from instance_0.
            # run_1 (config_1) -> on_update run_2 (config_2) -> rollback run_3 (config_1)
            #
            # Current behavior, start with instance which is not available or killed,
            # thereby instances which were on-going update will be rolled back first.
            # If an instance was in KILLED state (run-id=1) when it was picked up for rollback,
            # the run-id will be bumped up to 2 and move the config to the original one.
            # An instance with run-id=2 can either be on config_1 or config_2 (due to first update)
            # run_1 (config_1) -> (instance is in KILLED state, rollback runs first on it) rollback run_2 (config_1)
            continue
        else:
            assert False, (
                "unexpected run id %s" % t.assignedTask.taskId
            )
