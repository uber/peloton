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


def assert_pod_spec_changed(old_pod_spec, new_pod_spec):
    """
    assert that the command in the pod spec is different
    """
    assert old_pod_spec.containers[0].command.value != \
        new_pod_spec.containers[0].command.value


def assert_pod_spec_equal(old_pod_spec, new_pod_spec):
    """
    assert that the command in the pod spec is same
    """
    assert old_pod_spec.containers[0].command.value == \
        new_pod_spec.containers[0].command.value


def assert_pod_id_changed(old_pod_infos, new_pod_infos):
    """""
    assert if the pod id in old_pod_infos change in
    new_pod_infos
    """
    # TODO: find a better way to get pod name
    old_pod_dict = {}
    new_pod_dict = {}

    for old_pod_info in old_pod_infos:
        split_index = old_pod_info.status.pod_id.value.rfind('-')
        pod_name = old_pod_info.status.pod_id.value[:split_index]
        old_pod_dict[pod_name] = old_pod_info.status.pod_id.value

    for new_pod_info in new_pod_infos:
        split_index = new_pod_info.status.pod_id.value.rfind('-')
        pod_name = new_pod_info.status.pod_id.value[:split_index]
        new_pod_dict[pod_name] = new_pod_info.status.pod_id.value

    for pod_name, pod_id in old_pod_dict.items():
        assert not new_pod_dict[pod_name] == pod_id
