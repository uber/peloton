import pytest
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    assert_keys_equal,
    get_job_update_request,
    get_update_status,
    start_job_update,
    wait_for_rolled_forward,
    wait_for_update_status,
    verify_host_limit_1,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__start_job_update_with_pulse(client):
    req = get_job_update_request("test_dc_labrat_pulsed.yaml")
    res = client.start_job_update(
        req, "start pulsed job update test/dc/labrat"
    )
    assert (
        get_update_status(client, res.key)
        == api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE
    )

    client.pulse_job_update(res.key)
    wait_for_update_status(
        client,
        res.key,
        {
            api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
            api.JobUpdateStatus.ROLLING_FORWARD,
        },
        api.JobUpdateStatus.ROLLED_FORWARD,
    )


def test__start_job_update_revocable_job(client):
    """
    Given 12 non-revocable cpus, and 12 revocable cpus
    Create a non-revocable of 3 instance, with 3 CPU per instance
    Create a revocable job of 1 instance, with 4 CPU per instance
    """
    non_revocable_job = start_job_update(
        client,
        "test_dc_labrat_cpus_large.yaml",
        "start job update test/dc/labrat_large",
    )

    revocable_job = start_job_update(
        client,
        "test_dc_labrat_revocable.yaml",
        "start job update test/dc/labrat_revocable",
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    # validate 1 revocable tasks are running
    res = client.get_tasks_without_configs(
        api.TaskQuery(
            jobKeys={revocable_job}, statuses={api.ScheduleStatus.RUNNING}
        )
    )
    assert len(res.tasks) == 1

    # validate 3 non-revocable tasks are running
    res = client.get_tasks_without_configs(
        api.TaskQuery(
            jobKeys={non_revocable_job}, statuses={api.ScheduleStatus.RUNNING}
        )
    )
    assert len(res.tasks) == 3


def test__failed_update(client):
    """
    update failed
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_bad_config.yaml"),
        "rollout bad config",
    )

    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.FAILED,
    )


def test__start_job_update_with_msg(client):
    update_msg = "update msg 1"
    job_key = start_job_update(client, "test_dc_labrat.yaml", update_msg)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=job_key)
    )

    assert len(res.detailsList) == 1

    # verify events are sorted ascending
    assert len(res.detailsList[0].updateEvents) > 0
    update_events_ts = [e.timestampMs for e in res.detailsList[0].updateEvents]
    assert update_events_ts == sorted(update_events_ts)
    assert len(res.detailsList[0].instanceEvents) > 0
    instance_events_ts = [
        e.timestampMs for e in res.detailsList[0].instanceEvents
    ]
    assert instance_events_ts == sorted(instance_events_ts)

    assert (
        res.detailsList[0].updateEvents[0].status
        == api.JobUpdateStatus.ROLLING_FORWARD
    )
    assert res.detailsList[0].updateEvents[0].message == update_msg
    assert (
        res.detailsList[0].updateEvents[-1].status
        == api.JobUpdateStatus.ROLLED_FORWARD
    )


def test__simple_update_with_no_diff(client):
    """
    test simple update use case where second update has no config
    change, thereby new workflow created will have no impact
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # Do update with same config, which will yield no impact
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml"),
        "start job update test/dc/labrat_large_job_diff_executor",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) == 0

    # Do another update with same config, which will yield no impact
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) == 0


def test__simple_update_with_diff(client):
    """
    test simple update use case where second update has config
    change, here all the instances will move to new config
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # Do update with labels changed
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    job_key = res.key.job
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    for task in res.tasks:
        assert task.ancestorId is not None

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) == 0


def test__override_rolling_forward_update(client):
    """
    Override an on-going update without config change, will abort current update
    and start latest one.
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=res.key.job)
    )

    assert len(res.detailsList) == 2
    # Previous rolling_forward update is aborted
    assert (
        res.detailsList[1].updateEvents[-1].status
        == api.JobUpdateStatus.ABORTED
    )


def test__override_rolling_forward_update_with_diff(client):
    """
    Override an on-going update with config change, will abort current update
    and start latest one.
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml"),
        "start job update test/dc/labrat_large_job_diff_labels",
    )
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(
        None, api.JobUpdateQuery(jobKey=res.key.job)
    )

    assert len(res.detailsList) == 2
    # Previous rolling_forward update is aborted
    assert (
        res.detailsList[1].updateEvents[-1].status
        == api.JobUpdateStatus.ABORTED
    )


def test__host_limit_1(client, hostmgr):
    """
    - Create a job with host limit 1 constraint and validate each pod
    is running on different host.
    - Update a job, wait for it to complete and verify host limit 1 constraint.
    - Update a job, restart host manager, then wait for update to complete and
    lastly verify host limit 1 constraint.
    """
    # Create job.
    job_key = start_job_update(
        client, "test_dc_labrat.yaml", "start job update test/dc/labrat"
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={job_key}))
    assert len(res.tasks) == 3

    verify_host_limit_1(res.tasks)

    # Start a update with host limit 1 constraint
    job_key = start_job_update(
        client, "test_dc_labrat_1.yaml", "start job update test/dc/labrat_1"
    )

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}))
    assert len(res.tasks) == 3

    verify_host_limit_1(res.tasks)

    # Start an update, and restart hostmanager before update completes.
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat.yaml"),
        "start job update test/dc/labrat",
    )

    # restart host manager
    hostmgr.restart()

    wait_for_rolled_forward(client, res.key)

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}))
    assert len(res.tasks) == 3

    verify_host_limit_1(res.tasks)


def test__start_job_update_with_get_jobs(client, aurorabridge):
    """
    Check GetJobs returns the correct results when queried by role, and after
    a new job is created. This is used to verify that StartJobUpdate would
    correctly invalidate role to job id cache.
    Meanwhile, the test will restart aurorabridge container at the end, and
    verify the cache will be populated correctly as well.
    """

    labrat0 = start_job_update(
        client,
        "test_dc_labrat0.yaml",
        "start job update test/dc/labrat0",
    )

    res = client.get_jobs(labrat0.role)
    assert len(res.configs) == 1
    assert list(res.configs)[0].taskConfig.job == labrat0

    # Verify that start a new job invalidates the job role to id cache,
    # and returns the correct result.
    labrat1 = start_job_update(
        client,
        "test_dc_labrat1.yaml",
        "start job update test/dc/labrat1",
    )

    res = client.get_jobs(labrat1.role)
    assert len(res.configs) == 2
    assert_keys_equal(
        [c.taskConfig.job for c in res.configs],
        [labrat0, labrat1],
    )

    # Verify that after aurorabridge container restart, the cache will still
    # be populated correctly and return the correct result.
    aurorabridge.restart()
    time.sleep(10)

    res = client.get_jobs(labrat1.role)
    assert len(res.configs) == 2
    assert_keys_equal(
        [c.taskConfig.job for c in res.configs],
        [labrat0, labrat1],
    )
