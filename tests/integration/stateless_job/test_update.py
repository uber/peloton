import pytest

from tests.integration.update import Update

pytestmark = [pytest.mark.stateless, pytest.mark.update]

UPDATE_STATELESS_JOB_FILE = "test_update_stateless_job.yaml"
UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_add_instances.yaml"
UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_update_and_add_instances.yaml"


def test__create_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert_task_update_change_applied(old_task_infos, new_task_infos)


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
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_update_change_applied(old_task_infos, new_task_infos)


def test__create_update_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert_task_update_change_applied(old_task_infos, new_task_infos)


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
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_update_change_applied(old_task_infos, new_task_infos)


def assert_task_mesos_id_changed(old_task_infos, new_task_infos):
    """""
    assert if the mesos ids in old_task_infos changes in
    new_task_infos
    """
    for instance_id, old_task_info in old_task_infos.items():
        old_mesos_id = old_task_info.runtime.mesosTaskId.value
        new_mesos_id = new_task_infos[instance_id].runtime.mesosTaskId.value
        assert old_mesos_id != new_mesos_id


def assert_task_cmd_changed(old_task_infos, new_task_infos):
    """""
    assert if the cmd in old_task_infos changes in new_task_infos
    """
    for instance_id, old_task_info in old_task_infos.items():
        old_config = old_task_info.config
        new_config = new_task_infos[instance_id].config
        assert old_config.command.value != new_config.command.value


def assert_task_update_change_applied(old_task_infos, new_task_infos):
    """""
    assert if the update makes all of the changes expected
    """
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_cmd_changed(old_task_infos, new_task_infos)
