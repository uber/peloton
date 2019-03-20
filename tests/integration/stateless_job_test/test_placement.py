import pytest
import time

from tests.integration.job import Job, kill_jobs, with_constraint, with_instance_count, \
    with_job_name, with_labels
from tests.integration.common import IntegrationTestConfig
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2
from peloton_client.pbgen.peloton.api.v0.task import task_pb2

# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default,
              pytest.mark.placement,
              pytest.mark.random_order(disabled=True)]


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
                job_file='test_stateless_job.yaml',
                config=IntegrationTestConfig(
                    max_retry_attempts=100,
                ),
                options=[
                    with_labels({
                        label_key: label_value,
                    }),
                    with_constraint(_label_constraint(label_key, label_value)),
                    with_instance_count(3),
                ]
            )

    job.create()

    job.wait_for_state(goal_state='RUNNING')
    # Determine if tasks run on different hosts
    hosts = set()
    for _, task in job.get_tasks().iteritems():
        task_info = task.get_info()
        hosts = hosts.union({task_info.runtime.host})

    kill_jobs([job])

    # Ensure that the tasks run on 3 different hosts
    assert len(hosts) == 3


def test__create_2_stateless_jobs_with_task_to_task_anti_affinity_between_jobs(): # noqa
    label_key = "job.name"

    jobs = []
    for i in range(2):
        job = Job(
            job_file='test_stateless_job.yaml',
            config=IntegrationTestConfig(
                max_retry_attempts=100,
            ),
            options=[
                with_labels({
                    label_key: "peloton_stateless_job%s" % i
                }),
                with_job_name('TestPelotonDockerJob_Stateless' + repr(i)),
                with_instance_count(1),
            ]
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
                                    key='job.name',
                                    value='peloton_stateless_job%s' % i,
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
                                    key='job.name',
                                    value='peloton_stateless_job%s' % ((i + 1) % 2),
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
        job.wait_for_state(goal_state='RUNNING')
        for _, task in job.get_tasks().iteritems():
            task_info = task.get_info()
            hosts = hosts.union(set({task_info.runtime.host}))

    kill_jobs(jobs)

    # Ensure that the tasks run on 2 different hosts
    assert len(hosts) == 2
