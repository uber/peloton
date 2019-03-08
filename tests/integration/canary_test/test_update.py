import logging
import pytest

from tests.integration.canary_test.util import patch_job
from tests.integration.stateless_job_test.util import assert_pod_id_changed

log = logging.getLogger(__name__)

# TODO (varung): update tests to adjust config based on job spec
# rather hard coding same value for each test


@pytest.mark.incremental
def test__add_instances(canary_job):
    spec = canary_job.get_spec()
    old_instance_count = spec.instance_count
    new_instance_count = spec.instance_count + 1
    spec.instance_count = new_instance_count

    log.info(
        "job_name: %s, job_id: %s, add instances from %s to %s",
        spec.name,
        canary_job.get_job_id(),
        old_instance_count,
        new_instance_count,
    )
    patch_job(canary_job, spec)
    assert new_instance_count == len(canary_job.get_pods())


@pytest.mark.incremental
def test__remove_instances(canary_job):
    spec = canary_job.get_spec()
    old_instance_count = spec.instance_count
    new_instance_count = spec.instance_count - 1
    spec.instance_count = new_instance_count

    log.info(
        "job_name: %s, job_id: %s, reduce instances from %s to %s",
        spec.name,
        canary_job.get_job_id(),
        old_instance_count,
        new_instance_count,
    )
    patch_job(canary_job, spec)
    assert new_instance_count == len(canary_job.get_pods())


# TODO (varung): cleanup using resource_spec
@pytest.mark.incremental
def test__update_resource_spec(canary_job):
    spec = canary_job.get_spec()
    spec.default_spec.containers[0].resource.cpu_limit = 0.7
    spec.default_spec.containers[0].resource.mem_limit_mb = 15
    spec.default_spec.containers[0].resource.disk_limit_mb = 25

    log.info(
        "job_name: %s, job_id: %s, updated_resources: %s",
        spec.name,
        canary_job.get_job_id(),
        spec.default_spec.containers[0].resource,
    )
    patch_job(canary_job, spec)
    new_spec = canary_job.get_spec()
    assert new_spec.default_spec.containers[0].resource.cpu_limit == 0.7
    assert new_spec.default_spec.containers[0].resource.mem_limit_mb == 15
    assert new_spec.default_spec.containers[0].resource.disk_limit_mb == 25


@pytest.mark.incremental
def test__restart_pods(canary_job):
    old_pod_infos = canary_job.query_pods()

    # TODO add back batch size after API update in peloton client
    # stateless_job.restart(batch_size=1)
    canary_job.restart()
    canary_job.wait_for_workflow_state(goal_state="SUCCEEDED")

    canary_job.wait_for_all_pods_running()

    new_pod_infos = canary_job.query_pods()
    # restart should kill and start already running instances
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
