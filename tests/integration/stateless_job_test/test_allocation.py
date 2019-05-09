import pytest
import random
import time

from tests.integration.stateless_update import StatelessUpdate

pytestmark = [
    pytest.mark.default,
    pytest.mark.stateless,
    pytest.mark.allocation,
    pytest.mark.random_order(disabled=True),
]


def test_allocation_create_job(stateless_job):
    """
    Create a job and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 50
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_create_job__restart_placement_engine_and_hostmgr(
        stateless_job,
        hostmgr,
        placement_engines,
):
    """
    1. Create a job
    2. Restart placement engine and host manager
    3. Wait for all pods to transit to running state and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 50
    stateless_job.create()
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__add_instances(stateless_job, in_place):
    """
    1. Create a job and verify the allocation is as expected.
    2. Update the job to increase the instance count and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 50
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__add_instances_restart_hostmgr_and_placement_engine(
        stateless_job,
        in_place,
        hostmgr,
        placement_engines,
):
    """
    1. Create a job
    2. Restart hostmgr and placement engines.
    3. Wait for job to come up and verify the allocation is as expected.
    4. Update the job to increase the instance count.
    5. Restart hostmgr and placement engines.
    6. Wait for all pods to transit to running state and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 50
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__remove_instances(stateless_job, in_place):
    """
    1. Create a job and verify the allocation is as expected.
    2. Update the job to reduce the instance count and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 50
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 30
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__remove_instances_restart_hostmgr_and_placement_engine(
        stateless_job,
        in_place,
        hostmgr,
        placement_engines,
):
    """
    1. Create a job
    2. Restart hostmgr and placement engines.
    3. Wait for job to come up and verify the allocation is as expected.
    4. Update the job to reduce the instance count
    5. Restart hostmgr and placement engines.
    6. Wait for all pods to transit to running state and  verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 50
    stateless_job.create()
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 30
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__add_and_remove_instances(stateless_job, in_place):
    """
    1. Create a job and verify the allocation is as expected.
    2. Update the job to increase the instance count and verify the allocation is as expected.
    3. Update the job to reduce the instance count and verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 50
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 10
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__add_and_remove_instances_restart_hostmgr_and_placement_engine(
        stateless_job,
        in_place,
        hostmgr,
        placement_engines,
):
    """
    1. Create a job
    2. Restart hostmgr and placement engines.
    3. Wait for job to come up and verify the allocation is as expected.
    4. Update the job to increase the instance count
    5. Restart hostmgr and placement engines.
    6. Wait for all pods to transit to running state and verify the allocation is as expected.
    7. Update the job to reduce the instance count
    8. Restart hostmgr and placement engines.
    9. Wait for all pods to transit to running stateand verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 50
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 10
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def test_allocation_update_job__overwrite_update_restart_hostmgr_and_placement_engine(
        stateless_job,
        in_place,
        hostmgr,
        placement_engines,
):
    """
    1. Create a job
    2. Restart hostmgr and placement engines.
    3. Wait for job to come up and verify the allocation is as expected.
    4. Update the job to increase the instance count
    5. Restart hostmgr and placement engines.
    6. Update the job to reduce the instance count
    7. Restart hostmgr and placement engines.
    8. Wait for all pods to transit to running stateand verify the allocation is as expected.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.create()
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    stateless_job.wait_for_all_pods_running()

    verify_allocation(stateless_job)

    stateless_job.job_spec.instance_count = 50
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)

    stateless_job.job_spec.instance_count = 10
    update = StatelessUpdate(
        stateless_job, updated_job_spec=stateless_job.job_spec)
    update.create(in_place=in_place)
    restart_hostmgr_and_placement_engine(hostmgr, placement_engines)
    update.wait_for_state(goal_state="SUCCEEDED")

    verify_allocation(stateless_job)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    assert stateless_job.pool.get_allocation("cpu") == 0


def verify_allocation(job):
    assert job.pool.get_allocation("cpu") - (
        job.get_spec().instance_count *
        job.get_spec().default_spec.containers[0].resource.cpu_limit
    ) < 0.01

    assert job.pool.get_allocation("memory") - (
        job.get_spec().instance_count *
        job.get_spec().default_spec.containers[0].resource.mem_limit_mb
    ) < 0.01

    assert job.pool.get_allocation("disk") - (
        job.get_spec().instance_count *
        job.get_spec().default_spec.containers[0].resource.disk_limit_mb
    ) < 0.01


def restart_hostmgr_and_placement_engine(hostmgr, placement_engines):
    hostmgr.restart()
    time.sleep(random.randint(1, 10))
    placement_engines.restart()
    time.sleep(random.randint(1, 10))
    placement_engines.restart()
    time.sleep(random.randint(1, 10))
    hostmgr.restart()
