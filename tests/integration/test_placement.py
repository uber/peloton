import pytest
import time

from job import IntegrationTestConfig, Job, kill_jobs
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2
from peloton_client.pbgen.peloton.api.v0.task import task_pb2

# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.placement]


def test__create_stateful_job():
    job = Job(job_file='test_stateful_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state()

    # ToDo: Clean up persistent volumes from the test.


def test__create_a_stateful_job_with_3_tasks_on_3_different_hosts():
    job = Job(job_file='test_stateful_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.instanceCount = 3
    job.create()

    job.wait_for_state(goal_state='RUNNING')
    # Determine if tasks run on different hosts
    hosts = set()
    for _, task in job.get_tasks().iteritems():
        task_info = task.get_info()
        hosts = hosts.union(set({task_info.runtime.host}))

    kill_jobs([job])

    # ToDo: Clean up persistent volumes from the test.

    # Ensure that the tasks run on 3 different hosts
    assert len(hosts) == 3


def test__create_2_stateful_jobs_with_task_to_task_anti_affinity_between_jobs():  # noqa
    jobs = []
    for i in range(2):
        job = Job(job_file='test_stateful_job.yaml',
                  config=IntegrationTestConfig(max_retry_attempts=100))
        job.job_config.name = 'TestPelotonDockerJob_PersistentVolume' + repr(i)
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
                                    key='stateful_job',
                                    value='mystore%s' % i,
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
                                    key='stateful_job',
                                    value='mystore%s' % ((i + 1) % 2),
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

    # ToDo: Clean up persistent volumes from the test.

    # Ensure that the tasks run on 2 different hosts
    assert len(hosts) == 2
