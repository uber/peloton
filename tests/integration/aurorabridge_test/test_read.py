from collections import defaultdict
import time
import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    assert_keys_equal,
    remove_duplicate_keys,
    start_job_update,
    wait_for_killed,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge,
              pytest.mark.random_order(disabled=True)]


def test__get_jobs__get_job_summary(client):
    # Verify no jobs are returned before jobs are created.
    res = client.get_job_summary('')
    assert len(res.summaries) == 0

    res = client.get_jobs('')
    assert len(res.configs) == 0

    # Create two jobs under same role.
    test_dc_labrat_key = start_job_update(
        client,
        'test_dc_labrat_read.yaml',
        'start job update test/dc/labrat')
    test_dc_labrat_0_key = start_job_update(
        client,
        'test_dc_labrat0.yaml',
        'start job update test/dc/labrat0')

    # Different role should not show up.
    start_job_update(
        client,
        'test2_dc2_labrat2.yaml',
        'start job update test2/dc2/labrat2')

    # Add some wait time for lucene index to build
    time.sleep(10)

    # reduce instance count by 1 for test/dc/labrat0
    client.kill_tasks(
        test_dc_labrat_0_key,
        {0},
        'killing instance 0 for task test/dc/labrat0')
    wait_for_killed(client, test_dc_labrat_0_key, {0})

    # Ensure get_job_summary returns both jobs under role=test.
    res = client.get_job_summary(test_dc_labrat_key.role)
    assert len(res.summaries) == 2

    assert_keys_equal(
        [s.job.key for s in res.summaries],
        [test_dc_labrat_key, test_dc_labrat_0_key])

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
        [test_dc_labrat_key, test_dc_labrat_0_key])

    for c in res.configs:
        if c.key == test_dc_labrat_0_key:
            assert c.instanceCount == 1
        else:
            assert c.instanceCount == 2


def test__get_tasks_without_configs(client):
    # Create job.
    job_key = start_job_update(
        client,
        'test_dc_labrat_read.yaml',
        'start job update test/dc/labrat')

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
        assert 'test' == t.assignedTask.task.job.role
        assert 'dc' == t.assignedTask.task.job.environment
        assert 'labrat' == t.assignedTask.task.job.name
        assert 'testuser' == t.assignedTask.task.owner.user
        assert t.assignedTask.task.isService
        assert 5 == t.assignedTask.task.priority
        assert 'preemptible' == t.assignedTask.task.tier
        assert 2 == len(t.assignedTask.task.metadata)
        for m in t.assignedTask.task.metadata:
            if 'test_key_1' == m.key:
                assert 'test_value_1' == m.value
            elif 'test_key_2' == m.key:
                assert 'test_value_2' == m.value
            else:
                assert False, 'unexpected metadata {}'.format(m)
        assert 3 == len(t.assignedTask.task.resources)
        for r in t.assignedTask.task.resources:
            if r.numCpus > 0:
                assert 0.25 == r.numCpus
            elif r.ramMb > 0:
                assert 128 == r.ramMb
            elif r.diskMb > 0:
                assert 128 == r.diskMb
            else:
                assert False, 'unexpected resource {}'.format(r)
        assert 1 == len(t.assignedTask.task.constraints)
        assert 'host' == list(t.assignedTask.task.constraints)[0].name
        assert 1 == list(t.assignedTask.task.constraints)[0].constraint.limit.limit

        host_counts[t.assignedTask.slaveHost] += 1

    # Ensure the host limit is enforced.
    for host, count in host_counts.iteritems():
        assert count == 1, '{host} has more than 1 task'.format(host=host)


def test__get_tasks_without_configs_task_queries(client):
    # Verify no tasks are returned before creating.
    res = client.get_tasks_without_configs(api.TaskQuery())
    assert len(res.tasks) == 0

    # Create jobs.
    test_dc_labrat_key = start_job_update(
        client,
        'test_dc_labrat_read.yaml',
        'start job update test/dc/labrat')
    test_dc_labrat_0_key = start_job_update(
        client,
        'test_dc_labrat0.yaml',
        'start job update test/dc/labrat0')
    test_dc_0_labrat_1_key = start_job_update(
        client,
        'test_dc0_labrat1.yaml',
        'start job update test/dc0/labrat1')
    test_dc_labrat_1_key = start_job_update(
        client,
        'test_dc_labrat1.yaml',
        'start job update test/dc/labrat1')
    test2_dc2_labrat2_key = start_job_update(
        client,
        'test2_dc2_labrat2.yaml',
        'start job update test2/dc2/labrat2')

    # Add some wait time for lucene index to build
    time.sleep(10)

    # Kill one of the jobs.
    client.kill_tasks(
        test_dc_labrat_1_key,
        None,
        'killing all tasks test/dc/labrat1')
    wait_for_killed(client, test_dc_labrat_1_key)

    for message, query, expected_job_keys in [
        (
            'query job keys',
            api.TaskQuery(jobKeys={
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test2_dc2_labrat2_key,
            }),
            [
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test2_dc2_labrat2_key,
            ],
        ), (
            'query role + env + name',
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                environment=test_dc_labrat_key.environment,
                jobName=test_dc_labrat_key.name,
            ),
            [test_dc_labrat_key],
        ), (
            'query role + env',
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                environment=test_dc_labrat_key.environment,
            ),
            [
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test_dc_labrat_1_key,
            ],
        ), (
            'query role',
            api.TaskQuery(role=test_dc_labrat_key.role),
            [
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test_dc_labrat_1_key,
                test_dc_0_labrat_1_key,
            ],
        ), (
            'query role + statuses',
            api.TaskQuery(
                role=test_dc_labrat_key.role,
                statuses={api.ScheduleStatus.RUNNING},
            ),
            [
                test_dc_labrat_key,
                test_dc_labrat_0_key,
                test_dc_0_labrat_1_key,
            ],
        )
    ]:
        res = client.get_tasks_without_configs(query)
        # Expect 3 tasks per job key.
        assert len(res.tasks) == len(expected_job_keys) * 2, message
        assert_keys_equal(
            remove_duplicate_keys(t.assignedTask.task.job for t in res.tasks),
            expected_job_keys,
            message=message)
