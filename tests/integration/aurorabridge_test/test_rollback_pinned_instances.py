import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_auto_rolling_back,
    wait_for_rolled_back,
    wait_for_rolled_forward,
    wait_for_update_status,
    wait_for_killed,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__auto_rollback_with_pinned_instances(client):
    """
    1. Create a job.
    2. Start a bad update (version 2) targeting subset of instances.
    3. Wait for the instances to be auto-rolled back.
       Only the instances specified above should be affected.
    """
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "1"
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

    # start a bad update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_bad_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )
    pinned_req.settings.updateGroupSize = 5
    pinned_req.settings.maxFailedInstances = 3

    res = client.start_job_update(
        pinned_req,
        "start a bad update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_back(client, res.key)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    # verify that the run-id of only the pinned instances has changed
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "3"
        else:
            assert run_id == "1"

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for unaffected instances" % m
                )


def test__abort_auto_rollback_with_pinned_instances_and_update(client):
    """
    1. Create a job.
    2. Start a bad update (version 2) targeting subset of instances.
    3. Wait for the auto-rollback to kick-in.
    4. Once auto-rollback kicks in, abort the update.
    4. Start a new good update and wait for all instances to converge to that update.
    """
    # Create a job
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "1"
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

    # start a bad update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_bad_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )
    pinned_req.settings.maxFailedInstances = 4

    res = client.start_job_update(
        pinned_req,
        "start a bad update test/dc/labrat_large_job with pinned instances",
    )
    # wait for auto-rollback to kick-in
    wait_for_auto_rolling_back(client, res.key, timeout_secs=150)

    # abort the update
    client.abort_job_update(res.key, "abort update")
    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_BACK},
        api.JobUpdateStatus.ABORTED,
    )

    # start a new good update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_new_config.yaml"),
        "start job update test/dc/labrat_large_job with a good config",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 1
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_12":
                assert m.value == "test_value_12"
            else:
                assert False, "unexpected metadata %s" % m

        if t.assignedTask.instanceId in update_instances:
            # only a few of the pinned instances might have rolled back
            assert run_id == "3" or run_id == "4"
        else:
            assert run_id == "2"


@pytest.mark.skip(reason="flaky test")
def test__auto_rollback_with_pinned_instances__stopped_instances(client):
    """
    1. Create a job (v1).
    2. Start update  on the first subset of instances (v2).
    3. Start update on second subset of instances (v3).
    4. Stop some instances.
    5. Start a bad update on a subset consisting of at least
       one instance in each of v1, v2, v3 and stopped
    6. The instances should rollback to their respective previous good versions.
       The stopped instances in the bad update should transit to running.
    """
    all_instances = set([i for i in xrange(10)])
    # Create a job
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "1"
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

    # start a update on first subset of instances
    update_instances_1 = set([4, 5, 6, 7])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_diff_labels.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances_1]
    )

    res = client.start_job_update(
        pinned_req,
        "start job update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_forward(client, res.key)

    # Start another update on the second subset of instances
    update_instances_2 = set([8, 9])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_new_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances_2]
    )

    res = client.start_job_update(
        pinned_req,
        "start another job update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_forward(client, res.key)

    # Stop some instances
    stop_instances = set([5, 8])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing instance 5, 8 for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)

    # Start a bad update
    bad_update_instances = set([0, 5, 6, 9])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_bad_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in bad_update_instances]
    )
    pinned_req.settings.maxFailedInstances = 1
    pinned_req.settings.maxPerInstanceFailures = 1
    pinned_req.settings.updateGroupSize = 2

    res = client.start_job_update(
        pinned_req,
        "start a bad update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_back(client, res.key)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == (
        len(all_instances - stop_instances)
        + len(bad_update_instances.intersection(stop_instances))
    )

    for t in res.tasks:
        if t.assignedTask.instanceId in update_instances_1:
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, "unexpected metadata %s" % m
        elif t.assignedTask.instanceId in update_instances_2:
            print(t.assignedTask.instanceId)
            assert len(t.assignedTask.task.metadata) == 1
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_12":
                    assert m.value == "test_value_12"
                else:
                    assert False, "unexpected metadata %s" % m
        else:
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, "unexpected metadata %s" % m

        if t.assignedTask.instanceId in (
            stop_instances - bad_update_instances
        ):
            assert False, "unexpected start of stopped instance"


def test__auto_rollback_with_pinned_instances__add_instances(client):
    """
    1. Create a job.
    2. Start a bad update on a subset of instances and adding more instances.
    3. The instances should rollback to their previous version.
       No new instances should be added.
    """
    req = get_job_update_request("test_dc_labrat_large_job.yaml")
    req.instanceCount = 8
    res = client.start_job_update(
        req, "start job update test/dc/labrat_large_job"
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 8
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "1"
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

    # start a update with updateOnlyTheseInstances parameter,
    # and add instances
    update_instances = set([0, 2, 3, 8, 9])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_bad_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )
    pinned_req.settings.maxFailedInstances = 4

    res = client.start_job_update(
        pinned_req,
        "start a bad update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_back(client, res.key)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 8
    for t in res.tasks:
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "3"
        else:
            assert run_id == "1"


def test__auto_rollback_with_pinned_instances__remove_instances(client):
    """
    1. Create a job.
    2. Start a bad update on a subset of instances and adding more instances.
    3. The instances should rollback to their previous version.
       No instances should be removed.
    """
    req = get_job_update_request("test_dc_labrat_large_job.yaml")
    res = client.start_job_update(
        req, "start job update test/dc/labrat_large_job"
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "1"
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

    # start a update with updateOnlyTheseInstances parameter,
    # and reduce instance count
    update_instances = set([2, 3, 4, 5])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_bad_config.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )
    pinned_req.instanceCount = 8
    pinned_req.settings.maxFailedInstances = 3
    pinned_req.settings.updateGroupSize = 1

    res = client.start_job_update(
        pinned_req,
        "start a bad update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_back(client, res.key)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "3"
        else:
            assert run_id == "1"
