import pytest
import grpc

from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from tests.integration.update import Update

pytestmark = [pytest.mark.default, pytest.mark.stateless, pytest.mark.update]

UPDATE_STATELESS_JOB_FILE = "test_update_stateless_job.yaml"
UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_add_instances.yaml"
UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_update_and_add_instances.yaml"
INVALID_VERSION_ERR_MESSAGE = 'invalid job configuration version'


def test__create_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_add_instances(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5


def test__create_update_update_and_add_instances(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_add_instances_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5


def test__create_update_update_and_add_instances_with_batch(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_update_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    jobmgr.restart()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    try:
        update.create(config_version=10)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert e.details() == INVALID_VERSION_ERR_MESSAGE
        return
    raise Exception("configuration version mismatch error not received")


def test__create_update_stopped_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state='KILLED')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    stateless_job.start()
    update.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_stopped_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    ranges = task_pb2.InstanceRange(to=2)
    setattr(ranges, 'from', 0)
    stateless_job.stop(ranges=[ranges])
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.start()
    stateless_job.wait_for_state(goal_state='RUNNING')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_multiple_consecutive_updates(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update1 = Update(stateless_job,
                     updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update1.create()
    update2 = Update(stateless_job,
                     updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                     batch_size=1)
    update2.create()
    update2.wait_for_state(goal_state='SUCCEEDED')
    update1.wait_for_state(goal_state='ABORTED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__abort_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='ROLLING_FORWARD')
    update.abort()
    update.wait_for_state(goal_state='ABORTED')


def assert_task_mesos_id_changed(old_task_infos, new_task_infos):
    """""
    assert if the mesos ids in old_task_infos changes in
    new_task_infos
    """
    for instance_id, old_task_info in old_task_infos.items():
        old_mesos_id = old_task_info.runtime.mesosTaskId.value
        new_mesos_id = new_task_infos[instance_id].runtime.mesosTaskId.value
        assert old_mesos_id != new_mesos_id


def assert_task_config_changed(old_instance_config, new_instance_config):
    """""
    assert that the command in the instance config is different
    """
    assert old_instance_config.command.value != new_instance_config.command.value
