def assert_pod_id_equal(old_pod_infos, new_pod_infos):
    """""
    assert if the pod ids in old_pod_infos are equal to those in
    new_pod_infos
    """
    # TODO: find a better way to get pod name
    old_pod_dict = {}
    new_pod_dict = {}

    for old_pod_info in old_pod_infos:
        split_index = old_pod_info.status.pod_id.value.rfind("-")
        pod_name = old_pod_info.status.pod_id.value[:split_index]
        old_pod_dict[pod_name] = old_pod_info.status.pod_id.value

    for new_pod_info in new_pod_infos:
        split_index = new_pod_info.status.pod_id.value.rfind("-")
        pod_name = new_pod_info.status.pod_id.value[:split_index]
        new_pod_dict[pod_name] = new_pod_info.status.pod_id.value

    for pod_name, pod_id in old_pod_dict.items():
        assert new_pod_dict[pod_name] == pod_id


def assert_pod_spec_changed(old_pod_spec, new_pod_spec):
    """
    assert that the command in the pod spec is different
    """
    assert (
        old_pod_spec.containers[0].command.value
        != new_pod_spec.containers[0].command.value
    )


def assert_pod_spec_equal(old_pod_spec, new_pod_spec):
    """
    assert that the command in the pod spec is same
    """
    assert (
        old_pod_spec.containers[0].command.value
        == new_pod_spec.containers[0].command.value
    )


def assert_pod_id_changed(old_pod_infos, new_pod_infos):
    """""
    assert if the pod id in old_pod_infos change in
    new_pod_infos
    """
    # TODO: find a better way to get pod name
    old_pod_dict = {}
    new_pod_dict = {}

    for old_pod_info in old_pod_infos:
        split_index = old_pod_info.status.pod_id.value.rfind("-")
        pod_name = old_pod_info.status.pod_id.value[:split_index]
        old_pod_dict[pod_name] = old_pod_info.status.pod_id.value

    for new_pod_info in new_pod_infos:
        split_index = new_pod_info.status.pod_id.value.rfind("-")
        pod_name = new_pod_info.status.pod_id.value[:split_index]
        new_pod_dict[pod_name] = new_pod_info.status.pod_id.value

    for pod_name, pod_id in old_pod_dict.items():
        assert not new_pod_dict[pod_name] == pod_id


def get_host_to_task_count(hosts, job):
    host_task_count = {}

    for h in hosts:
        host_task_count[h] = 0

    for p in job.query_pods():
        if p.status.host in hosts:
            host_task_count[p.status.host] += 1

    return host_task_count
