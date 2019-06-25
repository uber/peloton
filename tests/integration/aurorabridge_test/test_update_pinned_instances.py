import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_killed,
    wait_for_rolled_forward,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__update_with_pinned_instances(client):
    """
    test basic pinned instance deployment:
    1. start a regular update (version 1) on all instances
    2. start another update (version 2) targeting subset of instances,
       expect only targeted instances to be updated
    3. start regular update (version 1) again on all instances, expect
       only instances affected by previous step to be updated
    """
    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
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

    # start a update with updateOnlyTheseInstances parameter
    update_instances = [0, 2, 3, 7, 9]
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

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "2"
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        else:
            assert run_id == "1"
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for unaffected instances" % m
                    )

    # start a regular update again should affect instances updated in
    # previous request
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml"),
        "start job update test/dc/labrat_large_job again (with executor data order diff)",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

        if t.assignedTask.instanceId in update_instances:
            assert run_id == "3"
        else:
            assert run_id == "1"


def test__update_with_pinned_instances__add_remove_instance(client):
    """
    test pinned instance deployment with add / remove instances:
    1. start a regular update (version 1) on all instances
    2. start another update (version 2) targeting subset of instances,
       while adding instances, expect only add and targeted instances
       to be updated
    3. start regular update (version 1) again on all instances, while
       removing instances, expect only instances affected by previous
       step to be updated and additional instances removed
    """
    all_instances = set(range(8))

    # start a regular update
    req = get_job_update_request("test_dc_labrat_large_job.yaml")
    req.instanceCount = 8
    res = client.start_job_update(
        req, "start job update test/dc/labrat_large_job"
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 8
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

    # start a update with updateOnlyTheseInstances parameter,
    # and add instances
    update_instances = set([0, 2, 3, 8, 9])
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

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 10
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            if t.assignedTask.instanceId in all_instances:
                assert run_id == "2"
            else:
                assert run_id == "1"

            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        else:
            assert run_id == "1"
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for unaffected instances" % m
                    )

    # start a regular update again should affect instances updated in
    # previous request, and remove instances
    req = get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml")
    req.instanceCount = 8
    res = client.start_job_update(
        req,
        "start job update test/dc/labrat_large_job again (with executor data order diff)",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 8
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, "unexpected metadata %s" % m

        if t.assignedTask.instanceId in (update_instances & all_instances):
            assert run_id == "3"
        else:
            assert run_id == "1"


def test__update_with_pinned_instances__stopped_instances(client):
    """
    test pinned instance deployment with stopped instances:
    1. start a regular update (version 1) on all instances
    2. stop subset of instances
    3. start another update (version 2) targeting subset of instances
       (stopped instances not included), expect only targeted instances
       to be updated and stopped instances remain stopped
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
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

    # stop subset of instances
    stop_instances = set([1, 6])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing instance 1, 6 for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances - stop_instances)
    for t in res.tasks:
        assert t.assignedTask.instanceId in (all_instances - stop_instances)

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

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances - stop_instances)

    # expect instance 0, 2, 3, 7, 9 to be updated to newer version, with run id 2
    # expect instance 1, 6 remain at stopped
    # expect instance 4, 5, 8 remain at original version, with run id 1
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "2"
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        elif t.assignedTask.instanceId in (all_instances - stop_instances):
            assert run_id == "1"
            assert len(t.assignedTask.task.metadata) == 2
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for unaffected instances" % m
                    )
        else:
            assert False, (
                "unexpected instance id %s: should be stopped"
                % t.assignedTask.instanceId
            )


def test__update_with_pinned_instances__start_stopped_instances(client):
    """
    test pinned instance deployment with stop / start instances:
    1. start a regular update (version 1) on all instances
    2. stop subset of instances
    3. start the same update (version 1) targeting subset of instances
       (stopped instances included), expect only stopped instances
       to start running, others unaffected
    4. start regular update (version 1) again on all instances, expect
       no change on all instances
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
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

    # stop subset of instances
    stop_instances = set([2, 8])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing instance 2, 8 for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances - stop_instances)
    for t in res.tasks:
        assert t.assignedTask.instanceId in (all_instances - stop_instances)

    # start a update with updateOnlyTheseInstances parameter
    # expect stopped instances to be started, others unchanged
    update_instances = set([2, 3, 5, 8])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_diff_executor.yaml"
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

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in (update_instances & stop_instances)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in stop_instances:
            assert run_id == "2"
        elif t.assignedTask.instanceId in (all_instances - stop_instances):
            assert run_id == "1"
        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for affected instances" % m
                )

    # start the regular update again same as the first one
    # expect no change for all instances
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start third job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) == 0

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in stop_instances:
            assert run_id == "2"
        elif t.assignedTask.instanceId in (all_instances - stop_instances):
            assert run_id == "1"
        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for affected instances" % m
                )


def test__update_with_pinned_instances__start_stopped_instances_all(client):
    """
    test pinned instance deployment with stop / start all instances:
    1. start a regular update (version 1) on all instances
    2. stop all instances
    3. start the same update (version 1) on all instances (stopped
       instances included), expect all instances to be updated and
       start running
    4. start regular update (version 1) again on all instances, expect
       no change on all instances
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
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

    # stop all instances
    stop_instances = set([i for i in xrange(10)])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing all instances for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == 0

    # start a update without updateOnlyTheseInstances parameter
    # expect all instances to be started
    update_instances = set([i for i in xrange(10)])

    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml"),
        "start second job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in (update_instances & stop_instances)

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert run_id == "2"

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for affected instances" % m
                )

    # start the regular update again same as the first one
    # expect no change for all instances
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start third job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) == 0

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        if t.assignedTask.instanceId in stop_instances:
            assert run_id == "2"
        elif t.assignedTask.instanceId in (all_instances - stop_instances):
            assert run_id == "1"
        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )

        assert len(t.assignedTask.task.metadata) == 2
        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for affected instances" % m
                )


def test__update_with_pinned_instances__deploy_stopped_instances(client):
    """
    test pinned instance deployment with stop / deploy instances:
    1. start a regular update (version 1) on all instances
    2. stop subset of instances
    3. start a new update (version 2) targeting subset of instances
       (stopped instances included), expect stopped instances to be
       brought up with new version and other targeted instances to
       be updated
    4. start regular update (version 1) again on all instances, expect
       only instances affected by previous step to be updated
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
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

    # stop subset of instances
    stop_instances = set([2, 8])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing instance 2, 8 for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances - stop_instances)
    for t in res.tasks:
        assert t.assignedTask.instanceId in (all_instances - stop_instances)

    # start a update with updateOnlyTheseInstances parameter
    # expect stopped instances to be started
    update_instances = set([2, 3, 5, 8])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_diff_labels.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )

    res = client.start_job_update(
        pinned_req,
        "start second job update test/dc/labrat_large_job with pinned instances and label diff",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "2"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        elif t.assignedTask.instanceId in (all_instances - update_instances):
            assert run_id == "1"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )

    # start the regular update again same as the first one
    # expect changes only for instances updated by previous update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job_diff_executor.yaml"),
        "start third job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "3"
        elif t.assignedTask.instanceId in (all_instances - update_instances):
            assert run_id == "1"
        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )

        for m in t.assignedTask.task.metadata:
            if m.key == "test_key_1":
                assert m.value == "test_value_1"
            elif m.key == "test_key_2":
                assert m.value == "test_value_2"
            else:
                assert False, (
                    "unexpected metadata %s for affected instances" % m
                )


def test__update_with_pinned_instances__deploy_stopped_instances_mixed(client):
    """
    test pinned instance deployment with mixed version and instance state
    1. start a regular update (version 1) on all instances
    2. stop subset of instances
    3. start a new update (version 2) targeting subset of instances
       (some of stopped instances included), expect targeted instances
       to be either brought up with newer version or updated with new
       version
    4. start regular update (version 1) again on another set of instances
       (some of previously stopped instances included, some of instances
       updated in previous step included), expect only stopped and
       instances affected previous step to be either brought up or updated
    """
    all_instances = set([i for i in xrange(10)])

    # start a regular update
    res = client.start_job_update(
        get_job_update_request("test_dc_labrat_large_job.yaml"),
        "start job update test/dc/labrat_large_job",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
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

    # stop subset of instances
    stop_instances = set([2, 8])
    client.kill_tasks(
        job_key,
        stop_instances,
        "killing instance 2, 8 for job test/dc/labrat_large_job",
    )
    wait_for_killed(client, job_key, stop_instances)
    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances - stop_instances)
    for t in res.tasks:
        assert t.assignedTask.instanceId in (all_instances - stop_instances)

    # start a update with updateOnlyTheseInstances parameter
    # expected only instances which targeted by updateOnlyTheseInstances
    # to be updated, within which stopped ones are started.
    update_instances = set([3, 5, 8])
    pinned_req = get_job_update_request(
        "test_dc_labrat_large_job_diff_labels.yaml"
    )
    pinned_req.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_instances]
    )

    res = client.start_job_update(
        pinned_req,
        "start second job update test/dc/labrat_large_job with pinned instances and label diff",
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        assert ie.instanceId in update_instances

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(
        (all_instances - stop_instances) | update_instances
    )
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2
        if t.assignedTask.instanceId in update_instances:
            assert run_id == "2"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        elif t.assignedTask.instanceId in (
            all_instances - stop_instances - update_instances
        ):
            assert run_id == "1"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )
        else:
            assert False, (
                "unexpected instance id %s: should be stopped"
                % t.assignedTask.instanceId
            )

    # start the regular update again same as the first one, targeting
    # subset of instances.
    # expect instance start / updated iff the instance has different config
    # or instance is stopped.
    update_2_instances = set([2, 3, 8, 9])
    pinned_req_2 = get_job_update_request(
        "test_dc_labrat_large_job_diff_executor.yaml"
    )
    pinned_req_2.settings.updateOnlyTheseInstances = set(
        [api.Range(first=i, last=i) for i in update_2_instances]
    )

    res = client.start_job_update(
        pinned_req_2, "start third job update test/dc/labrat_large_job"
    )
    wait_for_rolled_forward(client, res.key)
    job_key = res.key.job

    res = client.get_job_update_details(None, api.JobUpdateQuery(key=res.key))
    assert len(res.detailsList) == 1
    assert len(res.detailsList[0].instanceEvents) > 0
    for ie in res.detailsList[0].instanceEvents:
        # exclude instances that are previously running and still on
        # the first update
        assert ie.instanceId in (
            update_2_instances
            - (all_instances - update_instances - stop_instances)
        )

    # Expected instances for each corresponding state:
    #
    #   v1s  - instances on original job config (v1) and stopped
    #   v1r1 - instances on original job config (v1) and running with run id 1
    #   v1r2 - instances on original job config (v1) and running with run id 2
    #   v1r3 - instances on original job config (v1) and running with run id 3
    #   v2r2 - instances on updated job config (v2) and running with run id 2
    #
    # How did we calculate the instance ids?
    #
    # Let T1, T2, T3, T4 be each of the four operations, which are
    #   T1 - start original update (v1 job config) for all instances (let it be A)
    #   T2 - stop subset of instances (let it be S)
    #   T3 - start new update (v2 job config) on subset of instances (let it be U1)
    #   T4 - start origin update again (v1 job config) on subset of instances (let it be U2)
    #
    # At T1:
    #   v1r1 = A
    #
    # At T2:
    #   v1s = S
    #   v1r1' = v1r1 - S = A - S
    #
    # At T3:
    #   v1s' = v1s - U1 = S - U1
    #   v2r1 = (empty set)
    #   v2r2 = U1
    #   v1r1'' = A - v2r2 - v1s' = A - U1 - (S - U1)
    #
    # At T4:
    #   v1s'' = v1s' - U2 = S - U1 - U2
    #   v1r2 = U2 & v1s' = U2 & (S - U1)
    #   v1r3 = U1 & U2
    #   v2r2' = v2r2 - U2 = U1 - U2
    #   v1r1''' = A - v1s'' - v1r2 - v1r3 - v2r2'
    v1s = stop_instances - update_instances - update_2_instances
    v1r2 = update_2_instances & (stop_instances - update_instances)
    v1r3 = update_instances & update_2_instances
    v2r2 = update_instances - update_2_instances
    v1r1 = all_instances - v1s - v1r2 - v1r3 - v2r2

    assert not v1s, "should not be any instances remain as stopped"
    assert v1r1, "expect instances to be in version 1 run id 1"
    assert v1r2, "expect instances to be in version 1 run id 2"
    assert v1r3, "expect instances to be in version 1 run id 3"
    assert v2r2, "expect instances to be in version 2 run id 2"

    res = client.get_tasks_without_configs(
        api.TaskQuery(jobKeys={job_key}, statuses={api.ScheduleStatus.RUNNING})
    )
    assert len(res.tasks) == len(all_instances)
    for t in res.tasks:
        _, _, run_id = t.assignedTask.taskId.rsplit("-", 2)
        assert len(t.assignedTask.task.metadata) == 2

        if t.assignedTask.instanceId in v1r1:
            # version 1, run 1
            assert run_id == "1"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )

        elif t.assignedTask.instanceId in v1r2:
            # version 1, run 2
            assert run_id == "2"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )

        elif t.assignedTask.instanceId in v1r3:
            # version 1, run 3
            assert run_id == "3"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_1":
                    assert m.value == "test_value_1"
                elif m.key == "test_key_2":
                    assert m.value == "test_value_2"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )

        elif t.assignedTask.instanceId in v2r2:
            # version 2, run 2
            assert run_id == "2"
            for m in t.assignedTask.task.metadata:
                if m.key == "test_key_11":
                    assert m.value == "test_value_11"
                elif m.key == "test_key_22":
                    assert m.value == "test_value_22"
                else:
                    assert False, (
                        "unexpected metadata %s for affected instances" % m
                    )

        else:
            assert False, (
                "unexpected instance id %s" % t.assignedTask.instanceId
            )
