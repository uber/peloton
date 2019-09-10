import pytest
import time
import random
import logging

from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
    assert_pod_spec_changed,
)
from tests.integration.util import load_test_config
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.stateless_pb2 import JobSpec
from google.protobuf import json_format
from tests.integration.conftest import (
    maintenance,
    in_place,
)

from tests.integration.host import (
    get_host_in_state,
    wait_for_host_state,
    is_host_in_state,
)
from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v0.host import host_pb2
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2
log = logging.getLogger(__name__)


class TestJobMgrFailure(object):
    def test_jobmgr_failover(self, failure_tester):
        """
        Test job-manager fails over to follower when leader is
        restarted.
        """
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()

        # verify that we can create jobs after failover
        job = failure_tester.job(job_file="test_job_no_container.yaml")
        job.job_config.instanceCount = 10
        job.create()

        job.wait_for_state()

    def test_jobmgr_restart_job_succeeds(self, failure_tester):
        """
        Restart job-manager leader after creating a job and verify that
        the job runs to completion.
        """
        job = failure_tester.job(job_file="test_job_no_container.yaml")
        job.create()

        # Restart immediately, so that tasks will be in various
        # stages of launch
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        job.client = failure_tester.client

        job.wait_for_state()

    def test__stop_start_tasks_when_mesos_master_down_and_jobmgr_restarts_stateless_job(self, failure_tester):
        """
        Start and stop some tasks in a stateless job while mesos master is not running
        and job manager restarts, verify that those tasks start and stop as expected
        """
        # Step 1: Start a stateless job.
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()
        stateless_job.wait_for_state(goal_state="RUNNING")

        range = task_pb2.InstanceRange(to=1)
        setattr(range, "from", 0)

        # Step 2: Stop a subset of job instances when mesos master is down.
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        stateless_job.stop(ranges=[range])

        # Step 3: Restart job manager.
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        # Step 4: Start mesos master and wait for the instances to be stopped.
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)

        def wait_for_instance_to_stop():
            return stateless_job.get_task(0).state_str == "KILLED"
        stateless_job.wait_for_condition(wait_for_instance_to_stop)

        # Step 5: Start the same subset of instances when mesos master is down.
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        stateless_job.start(ranges=[range])

        # Step 6: Restart job manager.
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        # Step 7: Start mesos master and wait for the instances to transit to RUNNING.
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)

        def wait_for_instance_to_run():
            return stateless_job.get_task(0).state_str == "RUNNING"
        stateless_job.wait_for_condition(wait_for_instance_to_run)

    def test__stop_job_when_mesos_master_down_and_jobmgr_restarts_stateless_job(self, failure_tester):
        """
        Start and stop all tasks in a stateless job while mesos master is not running
        and job manager restarts, verify that all tasks start and stop as expected
        """
        # step 1: start the job
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()
        stateless_job.wait_for_state(goal_state="RUNNING")

        # Step 2: Stop all tasks in the job when mesos master is down.
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        stateless_job.stop()

        # Step 3: Restart job manager.
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        # Step 4: Start mesos master and wait for the job to terminate
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)
        stateless_job.wait_for_terminated()

    def test__stop_start_tasks_when_mesos_master_down_and_jobmgr_restarts_batch_job(self, failure_tester):
        """
        Start and stop some tasks in a job while mesos master is not running
        and job manager restarts, verify that those tasks start and stop as expected
        """
        # Step 1: start the job
        long_running_job = failure_tester.job(job_file="long_running_job.yaml")
        long_running_job.create()

        range = task_pb2.InstanceRange(to=1)
        setattr(range, "from", 0)

        # Step 2: stop some tasks in the job while mesos master is not running
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.stop(ranges=[range])

        leader1 = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader1
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader1)
        failure_tester.reset_client()
        long_running_job.client = failure_tester.client

        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)

        def wait_for_instance_to_stop():
            return long_running_job.get_task(0).state_str == "KILLED"
        long_running_job.wait_for_condition(wait_for_instance_to_stop)

        # Step 3: start the same tasks that were stopped while mesos master is not running
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.start(ranges=[range])

        leader2 = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader2
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader2)
        failure_tester.reset_client()
        long_running_job.client = failure_tester.client

        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)

        def wait_for_instance_to_run():
            return long_running_job.get_task(0).state_str == "RUNNING"
        long_running_job.wait_for_condition(wait_for_instance_to_run)

    def test__stop_job_when_mesos_master_down_and_jobmgr_restarts_batch_job(self, failure_tester):
        """
        Start and stop all tasks in a job while mesos master is not running
        and job manager restarts, verify that all tasks start and stop as expected
        """
        # Step 1: start the job
        long_running_job = failure_tester.job(job_file="long_running_job.yaml")
        long_running_job.create()

        # Step 2: stop all tasks in the job while mesos master is not running
        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.stop()

        # Step 3: restart the job manager and wait for leader change
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        long_running_job.client = failure_tester.client

        # Step 4: reconnect the client to the new job manager leader
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)
        long_running_job.wait_for_terminated()

    def test__start_restart_jobmgr(self, failure_tester):
        '''
        Restart job-manager leader while stateless job is starting
        and verify the start succeed
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()

        # TODO: remove this line after update and kill race
        # condition is fixed
        stateless_job.wait_for_all_pods_running()
        stateless_job.stop()
        stateless_job.wait_for_state(goal_state="KILLED")

        stateless_job.start()

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        stateless_job.wait_for_all_pods_running()

    def test__stop_restart_jobmgr(self, failure_tester):
        '''
        Restart job-manager leader while stateless job is stop
        and verify if the tasks in the job are changed
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()

        stateless_job.wait_for_state(goal_state="RUNNING")
        stateless_job.stop()

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        stateless_job.wait_for_state(goal_state="KILLED")

    def test__restart_restart_jobmgr(self, failure_tester, in_place):
        '''
        Restart job-manager leader while stateless job is restarted
        and verify if the tasks in the job are changed
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()
        stateless_job.wait_for_state(goal_state="RUNNING")

        old_pod_infos = stateless_job.query_pods()
        stateless_job.restart(in_place=in_place)

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        stateless_job.wait_for_workflow_state(goal_state="SUCCEEDED")

        stateless_job.wait_for_all_pods_running()

        new_pod_infos = stateless_job.query_pods()
        assert_pod_id_changed(old_pod_infos, new_pod_infos)

    def test__create_update_restart_jobmgr(self, failure_tester, in_place):
        '''
        Restart job-manager leader while stateless job is updated
        and verify if the tasks in the job are changed
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()
        stateless_job.wait_for_state(goal_state="RUNNING")

        old_pod_infos = stateless_job.query_pods()
        old_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()
        update = failure_tester.stateless_update(
            stateless_job,
            updated_job_file="test_update_stateless_job_update_and_add_instances_spec.yaml",
            batch_size=1,
        )
        update.create(in_place=in_place)

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        update.wait_for_state(goal_state="SUCCEEDED")
        new_pod_infos = stateless_job.query_pods()
        new_instance_zero_spec = stateless_job.get_pod(0).get_pod_spec()

        assert len(old_pod_infos) == 3
        assert len(new_pod_infos) == 5
        assert_pod_id_changed(old_pod_infos, new_pod_infos)
        assert_pod_spec_changed(old_instance_zero_spec, new_instance_zero_spec)

    @pytest.mark.parametrize("batch_size", [0, 1])
    def test__in_place_update_multi_component_restart(self, failure_tester, batch_size):
        '''
        Test in-place update can finish after multiple components restart
        '''
        # need extra retry attempts, since in-place update would need more time
        # to process given hostmgr would be restarted

        job1 = failure_tester.stateless_job()
        job1.create()
        job1.wait_for_all_pods_running()
        job2 = failure_tester.stateless_job()
        job2.create()
        job2.wait_for_all_pods_running()

        update1 = failure_tester.stateless_update(job1,
                                                  updated_job_file="test_update_stateless_job_spec.yaml",
                                                  batch_size=batch_size,
                                                  config=job1.config)
        update1.create(in_place=True)

        update2 = failure_tester.stateless_update(job2,
                                                  updated_job_file="test_update_stateless_job_spec.yaml",
                                                  batch_size=batch_size,
                                                  config=job2.config)
        update2.create()

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        job1.client = failure_tester.client
        job2.client = failure_tester.client

        time.sleep(random.randint(1, 10))

        leader = failure_tester.fw.get_leader_info(failure_tester.resmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.resmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.resmgr, leader)
        failure_tester.reset_client()
        job1.client = failure_tester.client
        job2.client = failure_tester.client

        time.sleep(random.randint(1, 10))

        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()
        job1.client = failure_tester.client
        job2.client = failure_tester.client

        time.sleep(random.randint(1, 10))

        assert 0 != failure_tester.fw.restart(failure_tester.stateless_pe)

        update1.wait_for_state(goal_state="SUCCEEDED")
        update2.wait_for_state(goal_state="SUCCEEDED")

    def test__in_place_update_success_rate_with_component_restart(self, failure_tester):
        '''
        Test in-place update can finish after multiple components restart
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.job_spec.instance_count = 30
        stateless_job.create()
        stateless_job.wait_for_all_pods_running()
        old_pod_infos = stateless_job.query_pods()

        job_spec_dump = load_test_config("test_update_stateless_job_spec.yaml")
        updated_job_spec = JobSpec()
        json_format.ParseDict(job_spec_dump, updated_job_spec)

        updated_job_spec.instance_count = 30
        update = failure_tester.stateless_update(stateless_job,
                                                 updated_job_spec=updated_job_spec,
                                                 batch_size=0)

        update.create(in_place=True)

        # restart all components except hostmgr
        leader1 = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader1
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader1)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        time.sleep(random.randint(1, 10))

        leader2 = failure_tester.fw.get_leader_info(failure_tester.resmgr)
        assert leader2
        assert 0 != failure_tester.fw.restart(failure_tester.resmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.resmgr, leader2)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client

        time.sleep(random.randint(1, 10))

        assert 0 != failure_tester.fw.restart(failure_tester.stateless_pe)

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

    # Test restart job manager when waiting for pod kill due to
    # host maintenance which can violate job SLA.
    @pytest.mark.skip("test is flaky")
    def test__host_maintenance_violate_sla_restart_jobmgr(self, failure_tester, maintenance):
        """
        1. Create a stateless job(instance_count=4) with host-limit-1 constraint and
        MaximumUnavailableInstances=1. Since there are only 3 UP hosts, one of
        the instances will not get placed (hence unavailable).
        2. Start host maintenance on one of the hosts (say A).
        3. Restart job manager.
        4. Since one instance is already unavailable, no more instances should be
        killed due to host maintenance. Verify that host A does not transition to DOWN
        """
        stateless_job = failure_tester.stateless_job()

        job_spec_dump = load_test_config('test_stateless_job_spec_sla.yaml')
        json_format.ParseDict(job_spec_dump, stateless_job.job_spec)
        stateless_job.job_spec.instance_count = 4
        stateless_job.create()
        stateless_job.wait_for_all_pods_running(num_pods=3)

        # Pick a host that is UP and start maintenance on it
        test_host1 = get_host_in_state(
            host_pb2.HOST_STATE_UP, failure_tester.client)
        # update the client in maintenance fixture
        maintenance["update_client"](failure_tester.client)
        resp = maintenance["start"]([test_host1])
        assert resp

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()
        stateless_job.client = failure_tester.client
        # update the client of maintainance
        maintenance["update_client"](failure_tester.client)

        try:
            wait_for_host_state(test_host1, host_pb2.HOST_STATE_DOWN)
            assert False, 'Host should not transition to DOWN'
        except:
            assert is_host_in_state(test_host1, host_pb2.HOST_STATE_DRAINING)
            assert len(stateless_job.query_pods(
                states=[pod_pb2.POD_STATE_RUNNING])) == 3
