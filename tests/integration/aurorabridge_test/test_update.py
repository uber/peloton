import pytest
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    get_update_status,
    start_job_update,
    wait_for_killed,
    wait_for_rolled_forward,
    wait_for_update_status,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge]


def test__start_job_update_with_pulse(client):
    req = get_job_update_request('test_dc_labrat_pulsed.yaml')
    res = client.start_job_update(req, 'start pulsed job update test/dc/labrat')
    assert get_update_status(client, res.key) == \
        api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE

    client.pulse_job_update(res.key)
    wait_for_update_status(
        client,
        res.key,
        {
            api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
            api.JobUpdateStatus.ROLLING_FORWARD,
        },
        api.JobUpdateStatus.ROLLED_FORWARD)


def test__start_job_update_revocable_job(client):
    """
    Given 12 non-revocable cpus, and 12 revocable cpus
    Create a non-revocable of 3 instance, with 3 CPU per instance
    Create a revocable job of 2 instance, with 2 CPU per instance
    """
    non_revocable_job = start_job_update(
        client,
        'test_dc_labrat_cpus_large.yaml',
        'start job update test/dc/labrat_large')

    revocable_job = start_job_update(
        client,
        'test_dc_labrat_revocable.yaml',
        'start job update test/dc/labrat_revocable')

    # Add some wait time for lucene index to build
    time.sleep(10)

    # validate 3 non-revocable tasks are running
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={non_revocable_job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 3

    # validate 2 revocable tasks are running
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={revocable_job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 2


def test__failed_update(client):
    """
    update failed
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_bad_config.yaml'),
        'rollout bad config')

    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.FAILED)


def test__start_job_update_with_msg(client):
    update_msg = 'update msg 1'
    job_key = start_job_update(client, 'test_dc_labrat.yaml', update_msg)

    res = client.get_job_update_details(None, api.JobUpdateQuery(jobKey=job_key))

    assert len(res.detailsList) == 1

    # verify events are sorted ascending
    assert len(res.detailsList[0].updateEvents) > 0
    update_events_ts = [e.timestampMs for e in res.detailsList[0].updateEvents]
    assert update_events_ts == sorted(update_events_ts)
    assert len(res.detailsList[0].instanceEvents) > 0
    instance_events_ts = [e.timestampMs for e in res.detailsList[0].instanceEvents]
    assert instance_events_ts == sorted(instance_events_ts)

    assert res.detailsList[0].updateEvents[0].status == \
        api.JobUpdateStatus.ROLLING_FORWARD
    assert res.detailsList[0].updateEvents[0].message == update_msg
    assert res.detailsList[0].updateEvents[-1].status == \
        api.JobUpdateStatus.ROLLED_FORWARD


def test__simple_update_with_no_diff(client):
    """
    test simple update use case where second update has no config
    change, thereby new workflow created will have no impact
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # Do update with same config, which will yield no impact
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_executor.yaml'),
        'start job update test/dc/labrat_large_job_diff_executor')
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert res.detailsList[0].instanceEvents is None

    # Do another update with same config, which will yield no impact
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert res.detailsList[0].instanceEvents is None


def test__simple_update_with_diff(client):
    """
    test simple update use case where second update has config
    change, here all the instances will move to new config
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    # Do update with labels changed
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_labels.yaml'),
        'start job update test/dc/labrat_large_job')
    job_key = res.key.job
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert len(res.detailsList[0].instanceEvents) > 0

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}))
    for task in res.tasks:
        assert task.ancestorId is not None

    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_labels.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList[0].updateEvents) > 0
    assert res.detailsList[0].instanceEvents is None


def test__update_with_pinned_instances(client):
    """
    test simple update use case where second update has config
    change for specfic instances, and verify only affected
    instances are updated/restarted.
    """
    # start a regular update
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

    # start a update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
    pinned_req = get_job_update_request('test_dc_labrat_large_job_diff_labels.yaml')
    pinned_req.settings.updateOnlyTheseInstances = \
        set([api.Range(first=i, last=i) for i in update_instances])

    res = client.start_job_update(
        pinned_req,
        'start job update test/dc/labrat_large_job with pinned instances')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == '2'
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == 'test_key_11':
                    assert m.value == 'test_value_11'
                elif m.key == 'test_key_22':
                    assert m.value == 'test_value_22'
                else:
                    assert False, 'unexpected metadata %s for affected instances' % m
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

    # start a regular update again should affect instances updated in
    # previous request
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_executor.yaml'),
        'start job update test/dc/labrat_large_job again (with executor data order diff)')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == 'test_key_1':
                assert m.value == 'test_value_1'
            elif m.key == 'test_key_2':
                assert m.value == 'test_value_2'
            else:
                assert False, 'unexpected metadata %s' % m

        if t.assignedTask.instanceId in update_instances:
            assert run_id == '3'
        else:
            assert run_id == '1'


def test__update_with_pinned_instances__stopped_instances(client):
    """
    test simple update use case where second update has config
    change for specific instances, and verify only affected
    instances are updated/restarted, and that unaffected stopped instances
    should not be started.
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == len(all_instances)
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

    # stop subset of instances
    stop_instances = set([1, 6])
    client.kill_tasks(
        job_key,
        stop_instances,
        'killing instance 1, 6 for job test/dc/labrat_large_job')
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == len(all_instances - stop_instances)
    for t in res.tasks:
        assert t.assignedTask.instanceId in (all_instances - stop_instances)

    # start a update with updateOnlyTheseInstances parameter
    update_instances = set([0, 2, 3, 7, 9])
    pinned_req = get_job_update_request('test_dc_labrat_large_job_diff_labels.yaml')
    pinned_req.settings.updateOnlyTheseInstances = \
        set([api.Range(first=i, last=i) for i in update_instances])

    res = client.start_job_update(
        pinned_req,
        'start job update test/dc/labrat_large_job with pinned instances')
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={job_key},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == len(all_instances - stop_instances)

    # expect instance 0, 2, 3, 7, 9 to be updated to newer version, with run id 2
    # expect instance 1, 6 remain at stopped
    # expect instance 4, 5, 8 remain at original version, with run id 1
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit('-', 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == '2'
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == 'test_key_11':
                    assert m.value == 'test_value_11'
                elif m.key == 'test_key_22':
                    assert m.value == 'test_value_22'
                else:
                    assert False, 'unexpected metadata %s for affected instances' % m
        elif t.assignedTask.instanceId in (all_instances - stop_instances):
            assert run_id == '1'
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == 'test_key_1':
                    assert m.value == 'test_value_1'
                elif m.key == 'test_key_2':
                    assert m.value == 'test_value_2'
                else:
                    assert False, 'unexpected metadata %s for unaffected instances' % m
        else:
            assert False, 'unexpected instance id %s: should be stopped' \
                % t.assignedTask.instanceId


def test__simple_update_with_restart_component(
        client,
        jobmgr,
        resmgr,
        hostmgr,
        mesos_master):
    """
    Start an update, and restart jobmgr, resmgr, hostmgr & mesos master.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')

    # wait for sometime for jobmgr goal state engine to kick-in
    time.sleep(10)
    jobmgr.restart()

    # wait for sometime to enqueue gangs
    time.sleep(10)

    # clear any admission and queues
    resmgr.restart()
    time.sleep(10)

    # wait for sometime to acquire host lock
    time.sleep(20)

    # clear host `placing` lock
    hostmgr.restart()
    time.sleep(10)

    # restart mesos master to jumble up host manager state
    mesos_master.restart()

    wait_for_rolled_forward(client, res.key)
