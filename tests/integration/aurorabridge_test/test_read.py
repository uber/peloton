from collections import defaultdict
import time
import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    assert_keys_equal,
    expand_instance_range,
    get_job_update_request,
    get_running_tasks,
    remove_duplicate_keys,
    start_job_update,
    wait_for_killed,
    wait_for_rolled_forward,
    delete_jobs,
)
from tests.integration.stateless_job import list_jobs, StatelessJob

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__get_jobs__get_job_summary(client):
    # Verify no jobs are returned before jobs are created.
    res = client.get_job_summary("")
    assert len(res.summaries) == 0

    res = client.get_jobs("")
    assert len(res.configs) == 0

    # Create two jobs under same role.
    test_dc_labrat_key = start_job_update(
        client, "test_dc_labrat_read.yaml", "start job update test/dc/labrat"
    )
    test_dc_labrat_0_key = start_job_update(
        client, "test_dc_labrat0.yaml", "start job update test/dc/labrat0"
    )

    # Different role should not show up.
    start_job_update(
        client, "test2_dc2_labrat2.yaml", "start job update test2/dc2/labrat2"
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    # reduce instance count by 1 for test/dc/labrat0
    client.kill_tasks(
        test_dc_labrat_0_key,
        {0},
        "killing instance 0 for task test/dc/labrat0",
    )
    wait_for_killed(client, test_dc_labrat_0_key, {0})

    # Ensure get_job_summary returns both jobs under role=test.
    res = client.get_job_summary(test_dc_labrat_key.role)
    assert len(res.summaries) == 2, "{jobs}".format(
        jobs=[s.job.key for s in res.summaries]
    )

    assert_keys_equal(
        [s.job.key for s in res.summaries],
        [test_dc_labrat_key, test_dc_labrat_0_key],
    )

    for s in res.summaries:
        if s.job.key == test_dc_labrat_0_key:
            assert s.stats.activeTaskCount == 1
        else:
            assert s.stats.activeTaskCount == 2
        assert s.job.instanceCount == 2

    # Ensure get_jobs returns both jobs under role=test.
    res = client.get_jobs(test_dc_labrat_key.role)
    assert len(res.configs) == 2

    assert_keys_equal(
        [c.taskConfig.job for c in res.configs],
        [test_dc_labrat_key, test_dc_labrat_0_key],
    )

    for c in res.configs:
        if c.key == test_dc_labrat_0_key:
            assert c.instanceCount == 1
        else:
            assert c.instanceCount == 2


def test__get_tasks_without_configs(client):
    # Create job.
    job_key = start_job_update(
        client, "test_dc_labrat_read.yaml", "start job update test/dc/labrat"
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={job_key}))
    assert len(res.tasks) == 2

    host_counts = defaultdict(int)

    for t in res.tasks:
        # ScheduledTask
        assert api.ScheduleStatus.RUNNING == t.status
        assert t.ancestorId is None

        # ScheduledTask.TaskEvent
        assert api.ScheduleStatus.RUNNING == t.taskEvents[-1].status
        assert "peloton" == t.taskEvents[-1].scheduler

        # ScheduledTask.AssignedTask
        assert t.assignedTask.taskId is not None
        assert t.assignedTask.slaveId is not None
        assert t.assignedTask.slaveHost is not None
        assert t.assignedTask.instanceId in (0, 1)

        # ScheduledTask.AssignedTask.TaskConfig
        assert "test" == t.assignedTask.task.job.role
        assert "dc" == t.assignedTask.task.job.environment
        assert "labrat" == t.assignedTask.task.job.name
        assert "testuser" == t.assignedTask.task.owner.user
        assert t.assignedTask.task.isService
        assert 5 == t.assignedTask.task.priority
        assert "preemptible" == t.assignedTask.task.tier
        assert 2 == len(t.assignedTask.task.metadata)
        for m in t.assignedTask.task.metadata:
            if "test_key_1" == m.key:
                assert "test_value_1" == m.value
            elif "test_key_2" == m.key:
                assert "test_value_2" == m.value
            else:
                assert False, "unexpected metadata {}".format(m)
        assert 3 == len(t.assignedTask.task.resources)
        for r in t.assignedTask.task.resources:
            if r.numCpus > 0:
                assert 0.25 == r.numCpus
            elif r.ramMb > 0:
                assert 128 == r.ramMb
            elif r.diskMb > 0:
                assert 128 == r.diskMb
            else:
                assert False, "unexpected resource {}".format(r)
        assert 1 == len(t.assignedTask.task.constraints)
        assert "host" == list(t.assignedTask.task.constraints)[0].name
        assert (
            1
            == list(t.assignedTask.task.constraints)[0].constraint.limit.limit
        )

        host_counts[t.assignedTask.slaveHost] += 1

    # Ensure the host limit is enforced.
    for host, count in host_counts.iteritems():
        assert count == 1, "{host} has more than 1 task".format(host=host)


def test__get_tasks_without_configs_task_queries(client):
    # Verify no tasks are returned before creating.
    res = client.get_tasks_without_configs(api.TaskQuery())
    assert len(res.tasks) == 0

    # Create jobs.
    test_dc_labrat_key = start_job_update(
        client, "test_dc_labrat_read.yaml", "start job update test/dc/labrat"
    )
    test_dc_labrat_0_key = start_job_update(
        client, "test_dc_labrat0.yaml", "start job update test/dc/labrat0"
    )
    test_dc_0_labrat_1_key = start_job_update(
        client, "test_dc0_labrat1.yaml", "start job update test/dc0/labrat1"
    )
    test_dc_labrat_1_key = start_job_update(
        client, "test_dc_labrat1.yaml", "start job update test/dc/labrat1"
    )
    test2_dc2_labrat2_key = start_job_update(
        client, "test2_dc2_labrat2.yaml", "start job update test2/dc2/labrat2"
    )

    # Add some wait time for lucene index to build
    time.sleep(10)

    # Kill one of the jobs.
    client.kill_tasks(
        test_dc_labrat_1_key, None, "killing all tasks test/dc/labrat1"
    )
    wait_for_killed(client, test_dc_labrat_1_key)

    for message, query, expected_job_keys in [
        (
            "query job keys",
            api.TaskQuery(
                jobKeys={
                    test_dc_labrat_key,
                    test_dc_labrat_0_key,
                    test2_dc2_labrat2_key,
                }
            ),
            [test_dc_labrat_key, test_dc_labrat_0_key, test2_dc2_labrat2_key],
        ),
        (
            "query role + env + name",
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                environment=test_dc_labrat_key.environment,
                jobName=test_dc_labrat_key.name,
            ),
            [test_dc_labrat_key],
        ),
        (
            "query role + env",
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                environment=test_dc_labrat_key.environment,
            ),
            [test_dc_labrat_key, test_dc_labrat_0_key, test_dc_labrat_1_key],
        ),
        (
            "query role",
            api.TaskQuery(role=test_dc_labrat_key.role),
            [
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test_dc_labrat_1_key,
                test_dc_0_labrat_1_key,
            ],
        ),
        (
            "query role + statuses",
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                statuses={api.ScheduleStatus.RUNNING},
            ),
            [test_dc_labrat_key, test_dc_labrat_0_key, test_dc_0_labrat_1_key],
        ),
    ]:
        res = client.get_tasks_without_configs(query)
        # Expect 3 tasks per job key.
        assert len(res.tasks) == len(expected_job_keys) * 2, message
        assert_keys_equal(
            remove_duplicate_keys(t.assignedTask.task.job for t in res.tasks),
            expected_job_keys,
            message=message,
        )


def test__get_config_summary__with_pinned_instances(client):
    """
    test pinned instance update which divides instances to two sets of
    configs, and verify getConfigSummary endpoint returns the correct
    result.
    """
    all_instances = set(range(10))

    # start a regular update
    job_key = start_job_update(
        client,
        "test_dc_labrat_large_job.yaml",
        "start job update test/dc/labrat_large_job",
    )

    tasks = get_running_tasks(client, job_key)
    assert len(tasks) == 10

    # start a update with updateOnlyTheseInstances parameter
    update_instances = set([0, 2, 3, 7, 9])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_diff_labels.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )

    res = client.start_job_update(
        pinned_req,
        "start job update test/dc/labrat_large_job with pinned instances",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    tasks = get_running_tasks(client, job_key)
    assert len(tasks) == 10

    res = client.get_config_summary(job_key)
    assert len(res.summary.groups) == 2
    for group in res.summary.groups:
        instances = set(expand_instance_range(group.instances))

        if instances == update_instances:
            # instances updated in the second update
            assert len(group.config.metadata) == 2
            for m in group.config.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, "unexpected metadata %s" % m

        elif instances == all_instances - update_instances:
            # instances updated from the first update
            assert len(group.config.metadata) == 2
            for m in group.config.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, "unexpected metadata %s" % m

        else:
            assert False, "unexpected instance range: %s" % group.instances


def test__get_tasks_without_configs__previous_run(client):
    """
    test getTasksWithoutConfigs endpoint for tasks from previous runs:
    1. start a regular update (version 1) on all instances
    2. start a another update (version 2) on all instances
    """
    req1 = get_job_update_request("test_dc_labrat_large_job.yaml")
    req1.settings.updateGroupSize = 10

    req2 = get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml")
    req2.settings.updateGroupSize = 10

    # start a regular update
    job_key = start_job_update(
        client, req1, "start job update test/dc/labrat_large_job"
    )

    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={job_key}))
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

    # start 6 new updates (assuming pod_runs_depth is 6), expect run id 1
    # to be excluded
    start_job_update(client, req2, "start job update test/dc/labrat_large_job")
    start_job_update(client, req1, "start job update test/dc/labrat_large_job")
    start_job_update(client, req2, "start job update test/dc/labrat_large_job")
    start_job_update(client, req1, "start job update test/dc/labrat_large_job")
    start_job_update(client, req2, "start job update test/dc/labrat_large_job")
    start_job_update(client, req1, "start job update test/dc/labrat_large_job")

    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={job_key}))
    assert len(res.tasks) == 10 * 6
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2

        if run_id in ("7"):
            assert t.status == api.ScheduleStatus.RUNNING
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, "unexpected metadata %s" % m
        elif run_id in ("6", "4", "2"):
            assert t.status == api.ScheduleStatus.KILLED
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, "unexpected metadata %s" % m
        elif run_id in ("5", "3"):
            assert t.status == api.ScheduleStatus.KILLED
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, "unexpected metadata %s" % m
        else:
            assert False, "unexpected run id: %d" % run_id


def test__get_job_update_details__filter_non_update_workflow(client):
    """
    test getJobUpdateDetails endpoint for filtering non-update workflows
    """
    req1 = get_job_update_request("test_dc_labrat_large_job.yaml")
    req1.settings.updateGroupSize = 10

    req2 = get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml")
    req2.settings.updateGroupSize = 10

    # start a regular update
    job_key = start_job_update(
        client, req1, "start job update test/dc/labrat_large_job")

    # trigger an unexpected restart through peloton api
    jobs = list_jobs()
    assert len(jobs) == 1

    job = StatelessJob(job_id=jobs[0].job_id.value)
    job.restart(batch_size=10)
    job.wait_for_workflow_state(goal_state="SUCCEEDED")     # wait for restart

    # start a new update
    start_job_update(client, req2, "start job update test/dc/labrat_large_job")

    # verify getJobUpdateDetails response
    res = client.get_job_update_details(
        None, api.JobUpdateQuery(role=job_key.role))
    assert len(res.detailsList) == 2

    for i, detail in enumerate(res.detailsList):
        if i == 0:
            assert len(detail.update.instructions.initialState) > 0
            for initial in detail.update.instructions.initialState:
                assert initial.task.metadata, 'Expect metadata to be present'
        else:
            assert len(detail.update.instructions.initialState) == 0


def test__get_job_update_details__deleted_job(client):
    """
    test JobMgr's private API - QueryJobCache (used by getJobUpdateDetails)
    won't crash if the job is deleted.
    """
    # start first update
    req1 = get_job_update_request("test_dc_labrat_large_job.yaml")
    req1.settings.updateGroupSize = 10

    job_key = start_job_update(
        client, req1, "start job update test/dc/labrat_large_job")

    # force delete job
    delete_jobs()

    # start second update
    req2 = get_job_update_request("test_dc_labrat_large_job_diff_labels.yaml")
    req2.settings.updateGroupSize = 10

    job_key = start_job_update(
        client, req2, "start job update test/dc/labrat_large_job")

    # verify getJobUpdateDetails response
    res = client.get_job_update_details(
        None, api.JobUpdateQuery(role=job_key.role))
    assert len(res.detailsList) == 1
