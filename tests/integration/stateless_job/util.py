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
