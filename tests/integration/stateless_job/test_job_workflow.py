import pytest
import grpc

from tests.integration.stateless_job.util import \
    assert_task_mesos_id_changed, assert_task_config_equal, assert_task_mesos_id_equal
from peloton_client.pbgen.peloton.api.v0.task import task_pb2

from tests.integration.update import Update

pytestmark = [pytest.mark.default, pytest.mark.stateless]


# test rolling start already running tasks with batch size,
# which should be noop
def test__rolling_start_running_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_start(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    # start should be a noop for already running instances
    assert_task_mesos_id_equal(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test rolling start on killed tasks with batch size
def test__rolling_start_killed_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_stop()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_start(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test rolling stop running tasks with batch size
def test__rolling_stop_running_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    resp = stateless_job.rolling_stop(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')


# test rolling stop killed tasks with batch size
def test__rolling_stop_killed_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    resp = stateless_job.rolling_stop(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_stop(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')


# test rolling start already running tasks without batch size,
# which should be noop
def test__rolling_start_running_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_start()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    # start should be a noop for already running instances
    assert_task_mesos_id_equal(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test rolling start on killed tasks without batch size
def test__rolling_start_killed_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_stop()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_start()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test rolling restart already running tasks with batch size,
def test__rolling_restart_running_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value

    resp = stateless_job.rolling_restart(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    # restart should kill and start already running instances
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)


# test rolling restart killed tasks with batch size,
def test__rolling_restart_killed_tasks_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    resp = stateless_job.rolling_stop()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_restart(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')


# test rolling restart running tasks without batch size,
def test__rolling_restart_running_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_restart()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    # restart should kill and start already running instances
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test rolling restart killed tasks without batch size,
def test__rolling_restart_killed_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config

    resp = stateless_job.rolling_stop()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_restart()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test calling rolling stop, rolling start, rolling restart
# and rolling stop in sequence
def test__rolling_stop_start_restart_stop(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    resp = stateless_job.rolling_stop(batch_size=1)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')

    resp = stateless_job.rolling_start()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value

    resp = stateless_job.rolling_restart()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')
    new_task_infos = stateless_job.list_tasks().value
    # make sure restart changed mesos id
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)

    resp = stateless_job.rolling_stop()
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')


# test calling rolling stop, rolling start, rolling restart
# and rolling stop on a partial set of tasks
def test__rolling_stop_start_restart_stop_partial_job(stateless_job):
    ranges = [task_pb2.InstanceRange(to=2)]
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    resp = stateless_job.rolling_stop(batch_size=1, ranges=ranges)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    for instance_id, task_info in stateless_job.list_tasks().value.items():
        task_state = task_info.runtime.state
        if instance_id < 2:
            assert task_state == task_pb2.KILLED
        else:
            assert task_state == task_pb2.RUNNING

    resp = stateless_job.rolling_start(batch_size=1, ranges=ranges)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    old_task_infos = stateless_job.list_tasks().value
    for instance_id, task_info in old_task_infos.items():
        assert task_info.runtime.state == task_pb2.RUNNING

    resp = stateless_job.rolling_restart(batch_size=1, ranges=ranges)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    for instance_id, old_task_info in old_task_infos.items():
        old_mesos_id = old_task_info.runtime.mesosTaskId.value
        new_mesos_id = new_task_infos[instance_id].runtime.mesosTaskId.value
        # only task in the ranges are restarted
        if instance_id < 2:
            assert old_mesos_id != new_mesos_id
        else:
            assert old_mesos_id == new_mesos_id

    resp = stateless_job.rolling_stop(batch_size=1, ranges=ranges)
    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    for instance_id, task_info in stateless_job.list_tasks().value.items():
        task_state = task_info.runtime.state
        if instance_id < 2:
            assert task_state == task_pb2.KILLED
        else:
            assert task_state == task_pb2.RUNNING


# test rolling restart succeeds during jobmgr restart
def test__rolling_restart_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    resp = stateless_job.rolling_restart(batch_size=1)

    jobmgr.restart()

    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')

    new_task_infos = stateless_job.list_tasks().value
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)


# TODO: enable this test after stateless recovery is done,
# now the job is not recovered due to it enters KILLED state
# def test__rolling_start_restart_jobmgr(stateless_job, jobmgr):
#     stateless_job.create()
#     stateless_job.rolling_stop()
#     stateless_job.wait_for_state(goal_state='KILLED')
#     resp = stateless_job.rolling_start(batch_size=1)
#     jobmgr.restart()
#     resp.workflow.wait_for_state(goal_state='SUCCEEDED')
#     stateless_job.wait_for_state(goal_state='RUNNING')


# test rolling stop succeeds during jobmgr restart
def test__rolling_stop_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    resp = stateless_job.rolling_stop(batch_size=1)

    jobmgr.restart()

    resp.workflow.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='KILLED')


# test rolling start fails due to concurrency error
# when a wrong resource version is provided
def test__rolling_start_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    try:
        stateless_job.rolling_start(resource_version=10)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    raise Exception("configuration version mismatch error not received")


# test rolling restart fails due to concurrency error
# when a wrong resource version is provided
def test__rolling_restart_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    try:
        stateless_job.rolling_restart(resource_version=10)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    raise Exception("configuration version mismatch error not received")


# test rolling stop fails due to concurrency error
# when a wrong resource version is provided
def test__rolling_stop_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    try:
        stateless_job.rolling_stop(resource_version=10)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    raise Exception("configuration version mismatch error not received")


# test the case that rolling start fails when a rolling start is
# already going on. large_stateless_job is used to make sure
# the first action is not finished when the second action is issued.
def test__rolling_start_fails_to_overwrite_rolling_start(large_stateless_job):
    large_stateless_job.create()
    large_stateless_job.wait_for_state(goal_state='RUNNING')

    resp = large_stateless_job.rolling_start(batch_size=1)
    try:
        large_stateless_job.rolling_start(batch_size=1)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    finally:
        resp.workflow.abort()

    raise Exception("workflow should not be able the overwrite the existing workflow")


# test the case that rolling start fails when an update that is
# already going on. large_stateless_job is used to make sure
# the first action is not finished when the second action is issued.
def test__rolling_start_fails_to_overwrite_update(large_stateless_job):
    large_stateless_job.create()
    large_stateless_job.wait_for_state(goal_state='RUNNING')
    update = Update(large_stateless_job,
                    updated_job_file="test_update_stateless_job_large.yaml")
    update.create()

    try:
        large_stateless_job.rolling_start(batch_size=1)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    finally:
        update.abort()

    Exception("workflow should not be able the overwrite the existing update")
