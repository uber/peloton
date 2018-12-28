from peloton_client.pbgen.peloton.api.v0.task import task_pb2


def assert_task_mesos_id_changed(old_task_infos, new_task_infos):
    """""
    assert if the mesos ids in old_task_infos change in
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


def assert_task_mesos_id_equal(old_task_infos, new_task_infos):
    """""
    assert if the mesos ids in old_task_infos are equal to those in
    new_task_infos
    """
    for instance_id, old_task_info in old_task_infos.items():
        old_mesos_id = old_task_info.runtime.mesosTaskId.value
        new_mesos_id = new_task_infos[instance_id].runtime.mesosTaskId.value
        assert old_mesos_id == new_mesos_id


def assert_task_config_equal(old_instance_config, new_instance_config):
    """""
    assert that the command in the instance config is equal
    """
    assert old_instance_config.command.value == new_instance_config.command.value


def assert_tasks_failed(task_infos):
    """
    assert that all instances are in FAILED state
    """
    for instance_id, task_info in task_infos.items():
        assert task_info.runtime.state == task_pb2.FAILED
