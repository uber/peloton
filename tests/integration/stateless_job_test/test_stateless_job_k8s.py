import pytest
import logging

from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.stateless_pb2 import (
    JobSpec,
)

from google.protobuf import json_format

from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
)
from tests.integration.stateless_update import StatelessUpdate
from tests.integration.util import load_test_config
from tests.integration.stateless_job import StatelessJob

pytestmark = [
    pytest.mark.k8s,
]

log = logging.getLogger(__name__)


# TODO: refactor existing stateless tests so they run with both mesos and k8s.
@pytest.mark.skip("issue with misc test running k8s tests")
def test__restart_killed_job():
    job = StatelessJob(job_file="test_stateless_job_spec_k8s.yaml")
    job.create()
    job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = job.query_pods()

    job.stop()
    job.wait_for_state(goal_state="KILLED")

    job.restart(in_place=False)

    job.wait_for_all_pods_running()

    new_pod_infos = job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test__in_place_update_success_rate tests that in-place update
# should succeed when every daemon is in healthy state.
# It starts a job with 30 instances, and start the in-place update
# without batch size, then it tests if any pod is running on unexpected
# host.
# TODO: refactor existing stateless tests so they run with both mesos and k8s.
@pytest.mark.skip("issue with misc test running k8s tests")
def test__in_place_update_success_rate():
    stateless_job = StatelessJob(job_file="test_stateless_job_spec_k8s.yaml")
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()
    old_pod_infos = stateless_job.query_pods()

    job_spec_dump = load_test_config("test_stateless_job_spec_k8s.yaml")
    updated_job_spec = JobSpec()
    json_format.ParseDict(job_spec_dump, updated_job_spec)

    updated_job_spec.instance_count = 30
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
    assert count == 0
