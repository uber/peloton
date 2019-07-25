import pytest
import time

from tests.integration.job import (
    Job,
    kill_jobs,
    with_constraint,
    with_instance_count,
    with_job_name,
    with_labels,
)
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import StatelessJob
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2
from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v1alpha import (
    peloton_pb2 as peloton_pb2_v1alpha,
)
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.placement,
    pytest.mark.random_order(disabled=True),
]


def _label_constraint(key, value):
    """
    Returns a label constraint for host limit 1
    :param key: The key fo the label
    :param value: The value of the label
    """
    return task_pb2.Constraint(
        type=1,
        labelConstraint=task_pb2.LabelConstraint(
            kind=1,
            condition=2,
            requirement=0,
            label=peloton_pb2.Label(
                # Tasks of my own job
                key=key,
                value=value,
            ),
        ),
    )


def test__create_a_stateless_job_with_3_tasks_on_3_different_hosts():
    label_key = "job.name"
    label_value = "peloton_stateless_job"

    job = Job(
        job_file="test_stateless_job.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
        options=[
            with_labels({label_key: label_value}),
            with_constraint(_label_constraint(label_key, label_value)),
            with_instance_count(3),
        ],
    )

    job.create()

    job.wait_for_state(goal_state="RUNNING")
    # Determine if tasks run on different hosts
    hosts = set()
    for _, task in job.get_tasks().iteritems():
        task_info = task.get_info()
        hosts = hosts.union({task_info.runtime.host})

    kill_jobs([job])

    # Ensure that the tasks run on 3 different hosts
    assert len(hosts) == 3


def test__create_2_stateless_jobs_with_task_to_task_anti_affinity_between_jobs():  # noqa
    label_key = "job.name"

    jobs = []
    for i in range(2):
        job = Job(
            job_file="test_stateless_job.yaml",
            config=IntegrationTestConfig(
                max_retry_attempts=100,
                pool_file='test_stateless_respool.yaml',
            ),
            options=[
                with_labels({label_key: "peloton_stateless_job%s" % i}),
                with_job_name("TestPelotonDockerJob_Stateless" + repr(i)),
                with_instance_count(1),
            ],
        )
        job.job_config.defaultConfig.constraint.CopyFrom(
            task_pb2.Constraint(
                type=2,
                andConstraint=task_pb2.AndConstraint(
                    constraints=[
                        task_pb2.Constraint(
                            type=1,
                            labelConstraint=task_pb2.LabelConstraint(
                                kind=1,
                                condition=2,
                                requirement=0,
                                label=peloton_pb2.Label(
                                    # Tasks of my own job
                                    key="job.name",
                                    value="peloton_stateless_job%s" % i,
                                ),
                            ),
                        ),
                        task_pb2.Constraint(
                            type=1,
                            labelConstraint=task_pb2.LabelConstraint(
                                kind=1,
                                condition=2,
                                requirement=0,
                                label=peloton_pb2.Label(
                                    # Avoid tasks of the other job
                                    key="job.name",
                                    value="peloton_stateless_job%s"
                                    % ((i + 1) % 2),
                                ),
                            ),
                        ),
                    ]
                ),
            )
        )
        jobs.append(job)

    for job in jobs:
        job.create()
        time.sleep(1)

    # Determine if tasks run on different hosts
    hosts = set()
    for job in jobs:
        job.wait_for_state(goal_state="RUNNING")
        for _, task in job.get_tasks().iteritems():
            task_info = task.get_info()
            hosts = hosts.union(set({task_info.runtime.host}))

    kill_jobs(jobs)

    # Ensure that the tasks run on 2 different hosts
    assert len(hosts) == 2


# Test placement of stateless job without exclusive constraint
# and verify that the tasks don't run on exclusive host
def test__placement_non_exclusive_job(exclusive_host):
    # We have 1 exclusive host and 2 non-exclusive hosts. Set number of
    # instances to be a few more than what can run simulatenously
    # on 2 non-exclusive hosts
    job = StatelessJob(job_file="test_stateless_job_cpus_large_spec.yaml")
    job.job_spec.instance_count = 10
    job.create()
    job.wait_for_state(goal_state="RUNNING")
    job.wait_for_all_pods_running(num_pods=5)

    job.stop()
    job.wait_for_terminated()

    # check that none of them ran on exclusive host
    pod_summaries = job.list_pods()
    for s in pod_summaries:
        if s.status.host:
            assert "exclusive" not in s.status.host


# Test placement of stateless job with exclusive constraint
# and verify that the tasks run only on exclusive host
def test__placement_exclusive_job(exclusive_host):
    excl_constraint = pod_pb2.Constraint(
        type=1,  # Label constraint
        label_constraint=pod_pb2.LabelConstraint(
            kind=2,  # Host
            condition=2,  # Equal
            requirement=1,
            label=peloton_pb2_v1alpha.Label(
                key="peloton/exclusive", value="exclusive-test-label"
            ),
        ),
    )
    # We have 1 exclusive host and 2 non-exclusive hosts. Set number of
    # instances to be a few more than what can run simulatenously on
    # a single exclusive host
    job = StatelessJob(job_file="test_stateless_job_cpus_large_spec.yaml")
    job.job_spec.default_spec.constraint.CopyFrom(excl_constraint)
    job.job_spec.instance_count = 6
    job.create()
    job.wait_for_state(goal_state="RUNNING")
    job.wait_for_all_pods_running(num_pods=4)

    job.stop()
    job.wait_for_terminated()

    # check that all of them ran on exclusive host
    pod_summaries = job.list_pods()
    for s in pod_summaries:
        if s.status.host:
            assert "exclusive" in s.status.host
