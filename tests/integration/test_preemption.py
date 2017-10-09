import pytest

from job import IntegrationTestConfig, Job


pytestmark = [pytest.mark.preemption]

# Tests task preemption with the following scenario:
# Total cluster CPU resources (local pcluster) : 12
# 2 Jobs with 12 instances(1 CPU per instance)
# 2 Resource pools with 6 CPU reservation each
# Procedure:
# 1. Create respool_A
# 2. Create respool_B
# 3. Create job_1 in respool_A : Since there is no demand in respool_B ,
#                                respool_A gets all the resources of
#                                respool_B. i.e. respool_A has 12 CPUs now,
#                                so all 12 instances of respool_A(which require
#                                1 CPU each) should go to RUNNING state.
#
# 4. Create job_2 in respool_B : Now respool_B demand has demand so
#                                entitlement calculation should kick in and
#                                reduce the entitlement of respool_A(6 CPUs),
#                                and increase the entitlement of respool_B(6
#                                CPUs). Since respool_A has 12 tasks RUNNING,
#                                6 of them should be preempted to make space
#                                for tasks in respool_B


def test__preemption_tasks_reschedules_task():
    job_1 = Job(job_file='test_preemption_job.yaml',
                config=IntegrationTestConfig(
                    pool_file='test_preemption_pool_A.yaml'))
    job_1.ensure_respool()

    job_2 = Job(job_file='test_preemption_job.yaml',
                config=IntegrationTestConfig(
                    pool_file='test_preemption_pool_B.yaml'))
    job_2.ensure_respool()

    job_1.create()
    job_1.wait_for_state(goal_state='RUNNING')

    # we should have all 12 tasks in running state
    def all_running():
        return all(t.state == 8 for t in job_1.get_tasks().values())

    job_1.wait_for_condition(all_running)

    # 6(6 CPUs worth) tasks from job_1 should be preempted
    def task_preempted():
        count = 0
        for t in job_1.get_tasks().values():
            # tasks should be enqueued back by the jobmanager and once
            # enqueued they should transition to PENDING state
            if t.state == 2:
                count += 1
        return count == 6

    # starting the second job should change the entitlement calculation
    job_2.create()

    # 6 jobs should be preempted from job1 to make space for job2
    job_1.wait_for_condition(task_preempted)

    # job2 should start progressing
    job_2.wait_for_state(goal_state='RUNNING')
