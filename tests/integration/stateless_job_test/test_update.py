import pytest
import grpc
import logging
import operator
import time
import random

from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless import (
    stateless_pb2,
)
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.stateless_pb2 import (
    JobSpec,
)
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2
from peloton_client.pbgen.peloton.api.v0.host import host_pb2

from google.protobuf import json_format

from tests.integration.conftest import get_container
from tests.integration.conf_util import minicluster_type
from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
    assert_pod_spec_changed,
    assert_pod_spec_equal,
    assert_pod_id_equal,
    get_host_to_task_count,
)
from tests.integration.stateless_update import StatelessUpdate
from tests.integration.util import load_test_config
from tests.integration.stateless_job import StatelessJob
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import INVALID_ENTITY_VERSION_ERR_MESSAGE
from tests.integration.host import (
    get_host_in_state,
    wait_for_host_state,
    is_host_in_state,
    query_hosts,
)

pytestmark = [
    pytest.mark.default,
    pytest.mark.stateless,
    pytest.mark.update,
    pytest.mark.random_order(disabled=True),
]

log = logging.getLogger(__name__)

UPDATE_STATELESS_JOB_SPEC = "test_update_stateless_job_spec.yaml"
UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC = (
    "test_update_stateless_job_add_instances_spec.yaml"
)
UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC = (
    "test_update_stateless_job_update_and_add_instances_spec.yaml"
)
UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC = (
    "test_stateless_job_spec.yaml"
)
UPDATE_STATELESS_JOB_BAD_SPEC = "test_stateless_job_with_bad_spec.yaml"
UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_SPEC = (
    "test_stateless_job_failed_health_check_spec.yaml"
)
UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC = (
    "test_stateless_job_successful_health_check_spec.yaml"
)
UPDATE_STATELESS_JOB_INVALID_SPEC = "test_stateless_job_spec_invalid.yaml"
UPDATE_STATELESS_JOB_JOB_CONFIG_UPDATE_SPEC = (
    "test_stateless_job_job_config_update_spec.yaml"
)


def update_stateless_job_spec():
    if minicluster_type() != "k8s":
        return "test_update_stateless_job_spec.yaml"
    return "test_stateless_job_spec_k8s.yaml"


def test__create_update(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_update_add_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5


def test__create_update_update_and_add_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_update_update_start_paused(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        start_paused=True,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="PAUSED")
    update.resume()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_update_with_batch_size(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_SPEC, batch_size=1
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_update_add_instances_with_batch_size(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5


def test__create_update_update_and_add_instances_with_batch(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_update_bad_version(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    try:
        update.create(entity_version="1-2-3", in_place=in_place)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


def test__pause_update_bad_version(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    try:
        update.pause(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


def test__resume_update_bad_version(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        start_paused=True,
        batch_size=1,
    )
    update.create(in_place=in_place)
    try:
        update.resume(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


def test__abort_update_bad_version(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    try:
        update.abort(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


def test__create_update_stopped_job(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    old_pod_states = set()
    for pod_info in old_pod_infos:
        old_pod_states.add(pod_info.spec.pod_name.value)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    stateless_job.start()
    update.wait_for_state(goal_state="SUCCEEDED")
    stateless_job.wait_for_state(goal_state="RUNNING")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)

    # Only new instances should be RUNNING
    for pod_info in new_pod_infos:
        if pod_info.spec.pod_name.value in new_pod_infos:
            assert pod_info.status.state == pod_pb2.POD_STATE_KILLED
        else:
            assert pod_info.status.state == pod_pb2.POD_STATE_RUNNING


def test__create_update_stopped_tasks(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    stateless_job.stop()
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    stateless_job.wait_for_state(goal_state="KILLED")
    stateless_job.start()
    stateless_job.wait_for_state(goal_state="RUNNING")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__create_multiple_consecutive_updates(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update1 = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update1.create(in_place=in_place)
    update2 = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update2.create(in_place=in_place)
    update2.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert len(old_pod_infos) == 3
    assert len(new_pod_infos) == 5
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


def test__abort_update(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLING_FORWARD")
    update.abort()
    update.wait_for_state(goal_state="ABORTED")


def test__update_reduce_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == 3
    # first increase instances
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(new_pod_infos) == 5
    # now reduce instances
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(new_pod_infos) == 3
    # now increase back again
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(new_pod_infos) == 5


def test__update_reduce_instances_stopped_tasks(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == 3
    # first increase instances
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(new_pod_infos) == 5
    # now stop last 2 tasks
    ranges = task_pb2.InstanceRange(to=5)
    setattr(ranges, "from", 3)
    stateless_job.stop(ranges=[ranges])
    # now reduce instance count
    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(new_pod_infos) == 3


# test__create_update_bad_config tests creating an update with bad config
# without rollback
def test__create_update_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_BAD_SPEC,
        max_failure_instances=3,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)
    for pod_info in stateless_job.query_pods():
        assert pod_info.status.state == pod_pb2.POD_STATE_FAILED


# test__create_update_add_instances_with_bad_config
# tests creating an update with bad config and more instances
# without rollback
def test__create_update_add_instances_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = stateless_job.job_spec.instance_count + 3

    update = StatelessUpdate(
        stateless_job,
        batch_size=1,
        updated_job_spec=updated_job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")

    # only one instance should be added
    assert (
        len(stateless_job.query_pods())
        == stateless_job.job_spec.instance_count + 1
    )


# test__create_update_reduce_instances_with_bad_config
# tests creating an update with bad config and fewer instances
# without rollback
def test__create_update_reduce_instances_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = stateless_job.job_spec.instance_count - 1

    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=updated_job_spec,
        batch_size=1,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == len(new_pod_infos)


# test__create_update_with_failed_health_check
# tests an update fails even if the new task state is RUNNING,
# as long as the health check fails
def test__create_update_with_failed_health_check(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_SPEC,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")


# test__create_update_to_disable_health_check tests an update which
# disables healthCheck
def test__create_update_to_disable_health_check(in_place):
    job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.job_spec.default_spec.containers[0].liveness_check.enabled = False
    update = StatelessUpdate(
        job,
        updated_job_spec=job.job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")


# test__create_update_to_enable_health_check tests an update which
# enables healthCheck
def test__create_update_to_enable_health_check(in_place):
    job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.job_spec.default_spec.containers[0].liveness_check.enabled = False
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.job_spec.default_spec.containers[0].liveness_check.enabled = True
    update = StatelessUpdate(
        job,
        updated_job_spec=job.job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")


# test__create_update_to_unset_health_check tests an update to unset
# health check config
def test__create_update_to_unset_health_check(in_place):
    job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    update = StatelessUpdate(
        job,
        updated_job_file=UPDATE_STATELESS_JOB_SPEC,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")


# test__create_update_to_unset_health_check tests an update to set
# health check config for a job without health check set
def test__create_update_to_set_health_check(in_place):
    job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_SPEC,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    update = StatelessUpdate(
        job,
        updated_job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")


# test__create_update_to_change_health_check_config tests an update which
# changes healthCheck
def test__create_update_to_change_health_check_config(in_place):
    job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_SPEC,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.job_spec.default_spec.containers[0].liveness_check.enabled = False
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.job_spec.default_spec.containers[
        0
    ].liveness_check.initial_interval_secs = 2
    update = StatelessUpdate(
        job,
        updated_job_spec=job.job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")


# test__auto_rollback_update_with_bad_config tests creating an update with bad config
# with rollback
def test__auto_rollback_update_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_BAD_SPEC,
        roll_back_on_failure=True,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLED_BACK")
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test__auto_rollback_update_add_instances_with_bad_config
# tests creating an update with bad config and more instances
# with rollback
def test__auto_rollback_update_add_instances_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = stateless_job.job_spec.instance_count + 3

    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=updated_job_spec,
        roll_back_on_failure=True,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLED_BACK")
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    # no instance should be added
    assert (
        len(stateless_job.query_pods())
        == stateless_job.job_spec.instance_count
    )
    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test__auto_rollback_update_reduce_instances_with_bad_config
# tests creating an update with bad config and fewer instances
# with rollback
def test__auto_rollback_update_reduce_instances_with_bad_config(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = stateless_job.job_spec.instance_count - 1

    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=updated_job_spec,
        roll_back_on_failure=True,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLED_BACK")
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    # no instance should be removed
    assert (
        len(stateless_job.query_pods())
        == stateless_job.job_spec.instance_count
    )
    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test__auto_rollback_update_with_failed_health_check
# tests an update fails even if the new task state is RUNNING,
# as long as the health check fails
def test__auto_rollback_update_with_failed_health_check(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_SPEC,
        roll_back_on_failure=True,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLED_BACK")
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test__pause_resume_initialized_update test pause and resume
#  an update in initialized state
def test__pause_resume_initialized_update(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job, batch_size=1, updated_job_file=UPDATE_STATELESS_JOB_SPEC
    )
    update.create(in_place=in_place)
    # immediately pause the update, so the update may still be INITIALIZED
    update.pause()
    update.wait_for_state(goal_state="PAUSED")
    update.resume()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


# test__pause_resume_initialized_update test pause and resume an update
def test__pause_resume__update(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job, batch_size=1, updated_job_file=UPDATE_STATELESS_JOB_SPEC
    )
    update.create(in_place=in_place)
    # sleep for 1 sec so update can begin to roll forward
    time.sleep(1)
    update.pause()
    update.wait_for_state(goal_state="PAUSED")
    update.resume()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
    assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)


# test_manual_rollback manually rolls back a running update when
# the instance count is reduced in the rollback.
# Note that manual rollback in peloton is just updating to the
# previous job configuration
def test_manual_rollback_reduce_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create(in_place=in_place)
    # manually rollback the update
    update2 = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC,
    )
    update2.create(in_place=in_place)
    update2.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == len(new_pod_infos)
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test_manual_rollback manually rolls back a running update when
# the instance count is increased in the rollback
def test_manual_rollback_increase_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    old_pod_infos = stateless_job.query_pods()
    old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    # reduce instance count and then roll it back
    update2 = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC,
    )
    update2.create(in_place=in_place)
    update3 = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    update3.create(in_place=in_place)
    update3.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert len(old_pod_infos) == len(new_pod_infos)
    new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
    assert_pod_spec_equal(old_instance_zero_spec, new_instance_zero_spec)


# test_auto_rollback_reduce_instances
#  rolls back a failed update when
# the instance count is reduced in the rollback.
def test_auto_rollback_reduce_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    job_spec_dump = load_test_config(
        UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_SPEC
    )
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    # increase the instance count
    updated_job_spec.instance_count = stateless_job.job_spec.instance_count + 3

    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=updated_job_spec,
        roll_back_on_failure=True,
        max_instance_attempts=1,
        max_failure_instances=1,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLED_BACK")
    assert (
        len(stateless_job.query_pods())
        == stateless_job.job_spec.instance_count
    )


# test_update_create_failure_invalid_spec tests the
# update create failure due to invalid spec in request
def test_update_create_failure_invalid_spec(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_INVALID_SPEC
    )
    try:
        update.create(in_place=in_place)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        return
    raise Exception("job spec validation error not received")


# test_update_killed_job tests updating a killed job.
# The job should be updated but still remain in killed state
def test_update_killed_job(in_place):
    job = StatelessJob(job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC)
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.stop()
    job.wait_for_state(goal_state="KILLED")

    update = StatelessUpdate(
        job, updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")

    assert job.get_spec().instance_count == 3
    assert job.get_status().state == stateless_pb2.JOB_STATE_KILLED


# test_start_job_with_active_update tests
# starting a job with an active update
def test_start_job_with_active_update(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    assert len(stateless_job.query_pods()) == 3
    stateless_job.stop()

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )

    update.create(in_place=in_place)
    stateless_job.start()

    update.wait_for_state(goal_state="SUCCEEDED")
    stateless_job.wait_for_all_pods_running()
    assert len(stateless_job.query_pods()) == 5


# test_stop_running_job_with_active_update_add_instances tests
# stopping a running job with an active update(add instances)
def test_stop_running_job_with_active_update_add_instances(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    assert len(stateless_job.query_pods()) == 3

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLING_FORWARD")

    stateless_job.stop()
    update.wait_for_state(goal_state="SUCCEEDED")
    assert stateless_job.get_spec().instance_count == 5


# test_stop_running_job_with_active_update_remove_instances tests
# stopping a running job with an active update(remove instances)
def test_stop_running_job_with_active_update_remove_instances(in_place):
    stateless_job = StatelessJob(
        job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_SPEC
    )
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    assert len(stateless_job.query_pods()) == 5

    update = StatelessUpdate(
        stateless_job,
        updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_SPEC,
        batch_size=1,
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="ROLLING_FORWARD")

    stateless_job.stop()
    update.wait_for_state(goal_state="SUCCEEDED")
    assert stateless_job.get_spec().instance_count == 3


# test_stop_running_job_with_active_update_same_instance_count tests stopping
# a running job with an active update that doesn't change instance count
def test_stop_running_job_with_active_update_same_instance_count(
    stateless_job,
    in_place
):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    stateless_job.job_spec.default_spec.containers[
        0
    ].command.value = "sleep 100"
    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=stateless_job.job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
    )
    update.create(in_place=in_place)
    stateless_job.stop()
    update.wait_for_state(goal_state="SUCCEEDED")
    assert stateless_job.get_spec().instance_count == 3
    assert (
        stateless_job.get_spec().default_spec.containers[0].command.value
        == "sleep 100"
    )


# test__create_update_before_job_fully_created creates an update
# right after a job is created. It tests the case that job can be
# updated before it is fully created
def test__create_update_before_job_fully_created(stateless_job, in_place):
    stateless_job.create()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_SPEC
    )
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")
    assert (
        stateless_job.get_spec().default_spec.containers[0].command.value
        == "while :; do echo updated; sleep 10; done"
    )


# test__in_place_update_success_rate tests that in-place update
# should succeed when every daemon is in healthy state.
# It starts a job with 30 instances, and start the in-place update
# without batch size, then it tests if any pod is running on unexpected
# host.
# TODO: Re-enable k8s when it stops being flaky.
# @pytest.mark.k8s
def test__in_place_update_success_rate(stateless_job):
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()
    old_pod_infos = stateless_job.query_pods()

    job_spec_dump = load_test_config(update_stateless_job_spec())
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = 30
    if minicluster_type() == "k8s":
        updated_job_spec.default_spec.containers[0].resource.mem_limit_mb = 0.1

    update = StatelessUpdate(stateless_job,
                             updated_job_spec=updated_job_spec,
                             batch_size=0)
    update.create(in_place=True)
    update.wait_for_state(goal_state='SUCCEEDED')

    new_pod_infos = stateless_job.query_pods()

    old_pod_dict = {}
    new_pod_dict = {}

    for old_pod_info in old_pod_infos:
        split_index = old_pod_info.status.pod_id.value.rfind('-')
        pod_name = old_pod_info.status.pod_id.value[:split_index]
        old_pod_dict[pod_name] = old_pod_info.status.host

    for new_pod_info in new_pod_infos:
        split_index = new_pod_info.status.pod_id.value.rfind('-')
        pod_name = new_pod_info.status.pod_id.value[:split_index]
        new_pod_dict[pod_name] = new_pod_info.status.host

    count = 0
    for pod_name, pod_id in old_pod_dict.items():
        if new_pod_dict[pod_name] != old_pod_dict[pod_name]:
            log.info("%s, prev:%s, cur:%s", pod_name,
                     old_pod_dict[pod_name], new_pod_dict[pod_name])
            count = count + 1
    log.info("total mismatch: %d", count)
    assert count == 0


# test__in_place_kill_job_release_host tests the case of killing
# an ongoing in-place update would release hosts, so the second
# update can get completed
def test__in_place_kill_job_release_host():
    job1 = StatelessJob(
        job_file="test_stateless_job_spec.yaml",
    )
    job1.create()
    job1.wait_for_state(goal_state="RUNNING")

    job2 = StatelessJob(
        job_file="test_stateless_job_spec.yaml",
    )
    job2.create()
    job2.wait_for_state(goal_state="RUNNING")

    update1 = StatelessUpdate(job1,
                              updated_job_file=UPDATE_STATELESS_JOB_SPEC,
                              batch_size=0)
    update1.create(in_place=True)
    # stop the update
    job1.stop()

    update2 = StatelessUpdate(job2,
                              updated_job_file=UPDATE_STATELESS_JOB_SPEC,
                              batch_size=0)
    update2.create()

    # both updates should complete
    update1.wait_for_state(goal_state="SUCCEEDED")
    update2.wait_for_state(goal_state="SUCCEEDED")


@pytest.mark.skip(reason="flaky test")
def test__in_place_update_host_maintenance(stateless_job, maintenance):
    # add enough instances so each host should have some tasks running
    stateless_job.job_spec.instance_count = 9
    # need extra retry attempts, since in-place update would need more time
    # to process given agent is put in maintenance mode
    stateless_job.config = IntegrationTestConfig(
        max_retry_attempts=300,
        pool_file='test_stateless_respool.yaml',
    ),
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = 9
    update = StatelessUpdate(stateless_job,
                             updated_job_spec=updated_job_spec,
                             batch_size=0)
    update.create(in_place=True)

    # Pick a host that is UP and start maintenance on it
    test_host = get_host_in_state(host_pb2.HOST_STATE_UP)
    resp = maintenance["start"]([test_host])
    assert resp

    wait_for_host_state(test_host, host_pb2.HOST_STATE_DOWN)
    update.wait_for_state(goal_state="SUCCEEDED")


def test__update_with_sla_aware_host_maintenance(stateless_job, maintenance):
    """
    1. Create a stateless job with 3 instances.
    2. Create a job update to update the instance job with instance count 2,
    add host-limit-1 constraint and define sla with maximum_unavailable_instances=1
    3. Start host maintenance on one of the hosts
    4. The host should transition to DOWN and the update workflow should SUCCEED
    """
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    job_spec_dump = load_test_config('test_stateless_job_spec_sla.yaml')
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)
    updated_job_spec.instance_count = 2

    update = StatelessUpdate(stateless_job,
                             updated_job_spec=updated_job_spec,
                             batch_size=1)
    update.create()

    # Pick a host that is UP and start maintenance on it
    test_host = get_host_in_state(host_pb2.HOST_STATE_UP)
    resp = maintenance["start"]([test_host])
    assert resp

    update.wait_for_state(goal_state="SUCCEEDED")
    wait_for_host_state(test_host, host_pb2.HOST_STATE_DOWN)


def test__update_with_host_maintenance_and_agent_down(stateless_job, maintenance):
    """
    1. Create a large stateless job (that take up more than two-thirds of
       the cluster resources) with MaximumUnavailableInstances=2.
    2. Start host maintenance on one of the hosts (say A) having pods of the job.
       MaximumUnavailableInstances=2 ensures that not more than 2 pods are
       unavailable due to host maintenance at a time.
    3. Take down another host which has pods running on it. This will TASK_LOST
       to be sent for all pods on the host after 75 seconds.
    4. Start an update to modify the instance spec of one of the pods.
    5. Since TASK_LOST would cause the job SLA to be violated, instances on the
       host A should not be killed once LOST event is received. Verify that
       host A does not transition to DOWN.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.job_spec.default_spec.containers[0].resource.cpu_limit = 0.3
    stateless_job.job_spec.sla.maximum_unavailable_instances = 2
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    hosts = [h.hostname for h in query_hosts([]).host_infos]
    host_to_task_count = get_host_to_task_count(hosts, stateless_job)
    sorted_hosts = [t[0] for t in sorted(
        host_to_task_count.items(), key=operator.itemgetter(1), reverse=True)]

    # Pick a host that has pods running on it to start maintenance on it.
    test_host = sorted_hosts[0]
    # pick another host which has pods of the job to take down
    host_container = get_container([sorted_hosts[1]])

    try:
        host_container.stop()
        maintenance["start"]([test_host])

        stateless_job.job_spec.instance_spec[10].containers.extend(
            [pod_pb2.ContainerSpec(resource=pod_pb2.ResourceSpec(disk_limit_mb=20))])
        update = StatelessUpdate(stateless_job,
                                 updated_job_spec=stateless_job.job_spec,
                                 batch_size=0)
        update.create()
        update.wait_for_state(goal_state="SUCCEEDED")

        stateless_job.stop()

        wait_for_host_state(test_host, host_pb2.HOST_STATE_DOWN)
        assert False, 'Host should not transition to DOWN'
    except:
        assert is_host_in_state(test_host, host_pb2.HOST_STATE_DRAINING)
        pass
    finally:
        host_container.start()


def test__update_with_host_maintenance__bad_config(stateless_job, maintenance):
    """
    1. Create a stateless job with 6 instances. Wait for all instances to reach
       RUNNING state. This means that there is at least one host with 2 or more
       instances on it
    2. Start a bad job update with max failure tolerance of 1 and auto-rollback
       disabled.
    3. Start host maintenance on one of the hosts (say host A).
    4. Wait for the update to fail. There should be 2 instances unavailable.
    5. Since 2 instances are already unavailable and
       maximum_unavailable_instances=1, host maintenance should not proceed.
       Verify that the host A doesn't transition to DOWN.
    """
    stateless_job.job_spec.sla.maximum_unavailable_instances = 1
    stateless_job.job_spec.instance_count = 6
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    hosts = [h.hostname for h in query_hosts([]).host_infos]
    host_to_task_count = get_host_to_task_count(hosts, stateless_job)
    sorted_hosts = [t[0] for t in sorted(
        host_to_task_count.items(), key=operator.itemgetter(1), reverse=True)]

    job_spec_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_SPEC)
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)
    updated_job_spec.instance_count = 6
    updated_job_spec.sla.maximum_unavailable_instances = 1
    update = StatelessUpdate(
        stateless_job,
        updated_job_spec=updated_job_spec,
        max_failure_instances=1,
        max_instance_attempts=1,
        batch_size=2,
    )
    update.create()

    # Pick a host that has pods running on it to start maintenance on it.
    test_host = sorted_hosts[0]
    maintenance["start"]([test_host])

    update.wait_for_state(goal_state="FAILED", failed_state="SUCCEEDED")

    try:
        wait_for_host_state(test_host, host_pb2.HOST_STATE_DOWN)
        assert False, 'Host should not transition to DOWN'
    except:
        assert is_host_in_state(test_host, host_pb2.HOST_STATE_DRAINING)


# test__create_update_update_job_config tests update job level config
# would not trigger task restart
def test__create_update_update_job_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()
    old_pod_infos = stateless_job.query_pods()
    update = StatelessUpdate(
        stateless_job, updated_job_file=UPDATE_STATELESS_JOB_JOB_CONFIG_UPDATE_SPEC
    )
    update.create()
    update.wait_for_state(goal_state="SUCCEEDED")
    new_pod_infos = stateless_job.query_pods()
    assert_pod_id_equal(old_pod_infos, new_pod_infos)
