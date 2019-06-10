import pytest

from tests.integration.job import Job, kill_jobs
from tests.integration.common import IntegrationTestConfig
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from tests.integration.pool import Pool

pytestmark = [
    pytest.mark.default,
    pytest.mark.preemption,
    pytest.mark.random_order(disabled=True),
]


def respool(request, pool_file):
    pool = Pool(config=IntegrationTestConfig(pool_file=pool_file))

    # teardown
    def delete_pool():
        pool.delete()

    request.addfinalizer(delete_pool)
    return pool


@pytest.fixture(scope="function", autouse=True)
def respool_a(request):
    return respool(request, pool_file="test_preemption_pool_A.yaml")


@pytest.fixture(scope="function", autouse=True)
def respool_b(request):
    return respool(request, pool_file="test_preemption_pool_B.yaml")


# Tests task preemption with the following scenario:
# Total cluster CPU resources (local minicluster) : 12
# 2 Jobs with 12 instances(1 CPU per instance)
# 2 Resource pools with 6 CPU reservation each
# Procedure:
# 1. Create respool_a
# 2. Create respool_b
# 3. Create job_1 in respool_a : Since there is no demand in respool_b ,
#                                respool_a gets all the resources of
#                                respool_b. i.e. respool_a has 12 CPUs now,
#                                so all 12 instances of respool_a(which require
#                                1 CPU each) should go to RUNNING state.
#
# 4. Create job_2 in respool_b : Now respool_b demand has demand so
#                                entitlement calculation should kick in and
#                                reduce the entitlement of respool_a(6 CPUs),
#                                and increase the entitlement of respool_b(6
#                                CPUs). Since respool_a has 12 tasks RUNNING,
#                                6 of them should be preempted to make space
#                                for tasks in respool_b


def test__preemption_tasks_reschedules_task(respool_a, respool_b):
    p_job_a = Job(
        job_file="test_preemptible_job.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=5),
    )

    p_job_a.create()
    p_job_a.wait_for_state(goal_state="RUNNING")

    # we should have all 12 tasks in running state
    def all_running():
        return all(t.state == 8 for t in p_job_a.get_tasks().values())

    p_job_a.wait_for_condition(all_running)

    # 6(6 CPUs worth) tasks from p_job_a should be preempted
    def task_preempted():
        count = 0
        for t in p_job_a.get_tasks().values():
            # tasks should be enqueued back by the jobmanager and once
            # enqueued they should transition to PENDING state
            if t.state == task.PENDING:
                count += 1
        return count == 6

    p_job_b = Job(
        job_file="test_preemptible_job.yaml",
        pool=respool_b,
        config=IntegrationTestConfig(
            max_retry_attempts=100, sleep_time_sec=10
        ),
    )
    # starting the second job should change the entitlement calculation
    p_job_b.create()

    # 6 tasks should be preempted from job1 to make space for job2
    p_job_a.wait_for_condition(task_preempted)

    # p_job_b should succeed
    p_job_b.wait_for_state(goal_state="SUCCEEDED")

    kill_jobs([p_job_a, p_job_b])


# Tests revocable resources can be used by only preemptible jobs
# Scenario:
# 1. respool_a = Reservation(cpu=6), Entitlement(cpu=12)
# 2. start a non-preemptible(np_job1) job which uses all the reserved cpu's
# 3. start another non-preemptible(np_job2) job and make sure it can't run
# 4. start a preemptible job which can run because entitlement>reservation
# 5. stop np_job1
# 6. np_job2 should start running
def test_non_preemptible_job(respool_a):
    # start non-preemptible job using all of CPU reservation.
    np_job_a_1 = Job(
        job_file="test_non_preemptible_job.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(max_retry_attempts=100),
    )
    np_job_a_1.create()
    np_job_a_1.wait_for_state(goal_state="RUNNING")

    # the resource pools CPU allocation should be equal to the reservation.
    assert np_job_a_1.pool.get_reservation(
        "cpu") == np_job_a_1.pool.get_allocation("cpu")

    # start another non-preemptible job which should not be admitted as all
    # the reservation(CPU) of the resource pool is used up.
    np_job_a_2 = Job(
        job_file="test_non_preemptible_job.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=5),
    )
    np_job_a_2.create()
    np_job_a_2.wait_for_state(goal_state="PENDING")

    # start preemptible job which should start running.
    p_job_a = Job(
        job_file="test_job.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(max_retry_attempts=100),
    )
    p_job_a.create()
    p_job_a.wait_for_state(goal_state="RUNNING")

    # stop the first non-preemptible job.
    np_job_a_1.stop()
    np_job_a_1.wait_for_state(goal_state="KILLED")

    # make sure the second one completes.
    np_job_a_2.wait_for_state(goal_state="RUNNING")

    kill_jobs([np_job_a_2, p_job_a])


# # Tests preemption of preemptible task
# # Scenario
# # 1. respool_a = Reservation(cpu=6), Entitlement(cpu=12)
# # 2. start a preemptible and non-preemptible job in resource pool a
# # 3. allocation(respool_a) > reservation(respool_a)
# # 4. create respool_b = Reservation(cpu=6), Entitlement(0)
# # 5. create preemptible job in respool_b, this should increase the
# #    entitlement of respool_b and decrease entitlement of respool_a.
# #    Entitlement(cpu=6) for both resource pools.
# # 6. preemptible job in respool_a should be preempted, since
# #    allocation>entitlement
# # 7. preemptible job in respool_b should start running.
# def test__preemption_non_preemptible_task(respool_a, respool_b):
#     # Create 2 Jobs : 1 preemptible and 1 non-preemptible in respool A
#     p_job_a = Job(
#         job_file="test_preemptible_job.yaml",
#         pool=respool_a,
#         config=IntegrationTestConfig(
#             max_retry_attempts=100, sleep_time_sec=10
#         ),
#     )
#     p_job_a.update_instance_count(6)
#
#     np_job_a = Job(
#         job_file="test_preemptible_job.yaml",
#         pool=respool_a,
#         config=IntegrationTestConfig(),
#     )
#     np_job_a.job_config.sla.preemptible = False
#     np_job_a.update_instance_count(6)
#
#     # preemptible job takes 6 CPUs
#     p_job_a.create()
#
#     # non preemptible job takes 6 reserved CPUs
#     np_job_a.create()
#
#     p_job_a.wait_for_state("RUNNING")
#     np_job_a.wait_for_state("RUNNING")
#
#     # pool allocation is more than reservation
#     assert np_job_a.pool.get_reservation(
#         "cpu") < np_job_a.pool.get_allocation("cpu")
#
#     # Create another job in respool B
#     p_job_b = Job(
#         job_file="test_preemptible_job.yaml",
#         pool=respool_b,
#         config=IntegrationTestConfig(
#             max_retry_attempts=100, sleep_time_sec=10
#         ),
#     )
#     p_job_b.update_instance_count(6)
#
#     p_job_b.create()
#
#     # p_job_b should remain PENDING since all resources are used by
#     # p_job_a
#     p_job_b.wait_for_state("PENDING")
#
#     # p_job_a should be preempted and go back to PENDING
#     p_job_a.wait_for_state(goal_state="PENDING")
#
#     # np_job_a should keep RUNNING
#     np_job_a.wait_for_state("RUNNING")
#
#     def all_tasks_running():
#         count = 0
#         for t in np_job_a.get_tasks().values():
#             if t.state == task.RUNNING:
#                 count += 1
#         return count == 6
#
#     # p_job_b should start running
#     p_job_b.wait_for_condition(all_tasks_running)
#
#     # pool A allocation is equal to reservation
#     assert np_job_a.pool.get_reservation(
#         "cpu") == np_job_a.pool.get_allocation("cpu")
#
#     # pool B allocation is equal to reservation
#     assert p_job_b.pool.get_reservation(
#         "cpu") == p_job_b.pool.get_allocation("cpu")
#
#     # wait for p_job_b to finish
#     p_job_b.wait_for_state("SUCCEEDED")
#
#     # make sure p_job_a starts running
#     p_job_a.wait_for_state("RUNNING")
#
#     kill_jobs([p_job_a, np_job_a, p_job_b])


# Spark driver depends on the task goalstate to detetermine if the task was
# preempted. This test tests that if "the task is a batch task and has preemption
# policy set to kill on preempt then the task goaslstate changes to PREEMPTING before
# it is killed.
# TODO: avyas@ This is a short term fix
def test__preemption_spark_goalstate(respool_a, respool_b):
    p_job_a = Job(
        job_file="test_preemptible_job_preemption_policy.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(
            max_retry_attempts=100, sleep_time_sec=10
        ),
    )

    p_job_a.create()
    p_job_a.wait_for_state(goal_state="RUNNING")

    # we should have all 12 tasks in running state
    def all_running():
        return all(t.state == 8 for t in p_job_a.get_tasks().values())

    p_job_a.wait_for_condition(all_running)

    preempted_task_set = {}

    # 6(6 CPUs worth) tasks from p_job_a should be preempted
    def task_preempted():
        count = 0
        for t in p_job_a.get_tasks().values():
            # tasks should be KILLED since killOnPreempt is set to true
            if t.state == task.KILLED:
                count += 1
                preempted_task_set[t] = True
        return count == 6

    p_job_b = Job(
        job_file="test_preemptible_job.yaml",
        pool=respool_b,
        config=IntegrationTestConfig(),
    )
    # starting the second job should change the entitlement calculation
    p_job_b.create()

    # 6 jobs should be preempted from job1 to make space for job2
    p_job_a.wait_for_condition(task_preempted)

    # check the preempted tasks and check the runtime info.
    for t in preempted_task_set:
        assert t.state == task.KILLED
        assert t.goal_state == task.PREEMPTING

    kill_jobs([p_job_a, p_job_b])


# Tests preemption override at task level
# Scenario
# 1. respool_a = Reservation(cpu=6), Entitlement(cpu=12)
# 2. start a preemptible job in resource pool a which has all the even task IDs
#    overridden to be non-preemptible i.e. 6 tasks are preemptible and 6 tasks
#    non-preemptible.
# 4. create respool_b = Reservation(cpu=6), Entitlement(0)
# 5. create preemptible job in respool_b, this should increase the
#    entitlement of respool_b and decrease entitlement of respool_a.
#    Entitlement(cpu=6) for both resource pools.
# 6. preemptible job in respool_a should be preempted, since
#    allocation>entitlement.
# 7. check the instance_id of tasks which were preempted and not preempted to
#    assert the preemption config at task level.
def test__preemption_task_level(respool_a, respool_b):
    p_job_a = Job(
        job_file="test_preemptible_job_preemption_override.yaml",
        pool=respool_a,
        config=IntegrationTestConfig(
            max_retry_attempts=100, sleep_time_sec=10
        ),
    )

    p_job_a.create()
    p_job_a.wait_for_state(goal_state="RUNNING")

    # we should have all 12 tasks in running state
    def all_running():
        return all(t.state == 8 for t in p_job_a.get_tasks().values())

    p_job_a.wait_for_condition(all_running)

    # odd instance ids should be preempted
    expected_preempted_tasks = set([1, 3, 5, 7, 9, 11])
    # even instance ids should be running
    expected_running_tasks = set([0, 2, 4, 6, 8, 10])

    preempted_task_set, running_task_set = set([]), set([])

    # 6(6 CPUs worth) tasks from p_job_a should be preempted
    def task_preempted():
        preempted_task_set.clear()
        running_task_set.clear()
        preempted_count, running_count = 0, 0
        for t in p_job_a.get_tasks().values():
            # tasks should be KILLED since killOnPreempt is set to true
            if t.state == task.KILLED:
                preempted_count += 1
                preempted_task_set.add(t.instance_id)
            if t.state == task.RUNNING:
                running_count += 1
                running_task_set.add(t.instance_id)

        return running_count == 6 and preempted_count == 6

    p_job_b = Job(
        job_file="test_preemptible_job.yaml",
        pool=respool_b,
        config=IntegrationTestConfig(),
    )
    # starting the second job should change the entitlement calculation and
    # start preempting tasks from p_job_a
    p_job_b.create()

    # 6 tasks(odd instance ids) should be preempted from job1 to make space for job2
    p_job_a.wait_for_condition(task_preempted)

    # check the preempted tasks and check instance ids should be odd.
    assert preempted_task_set == expected_preempted_tasks
    assert running_task_set == expected_running_tasks

    # wait for p_job_b to start running
    p_job_b.wait_for_state("RUNNING")

    kill_jobs([p_job_a, p_job_b])
