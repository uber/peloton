from peloton_client.pbgen.peloton.api.v0.task import task_pb2


class TestMesosMasterFailure(object):

    def test__stop_start_tasks_when_mesos_master_down_kills_tasks_when_started(self, failure_tester):
        '''
        Start Mesos master after creating a long running job and stopping the job
        and verify that the job is still running
        1. Create stateless job.
        2. Wait for job state RUNNING.
        3. Stop a subset of job instances when mesos master is down.
        4. Start mesos master and wait for the instances to be stopped.
        5. Start the same subset of instances when mesos master is down.
        6. Start mesos master and wait for the instances to transit to RUNNING.
        7. Stop the job when mesos master is down.
        8. Start mesos master and wait for the job to terminate
        '''
        long_running_job = failure_tester.job(job_file="long_running_job.yaml")
        long_running_job.create()
        long_running_job.wait_for_state(goal_state="RUNNING")

        range = task_pb2.InstanceRange(to=1)
        setattr(range, "from", 0)

        def wait_for_instance_to_stop():
            return long_running_job.get_task(0).state_str == "KILLED"

        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.stop(ranges=[range])
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)
        long_running_job.wait_for_condition(wait_for_instance_to_stop)

        def wait_for_instance_to_run():
            return long_running_job.get_task(0).state_str == "RUNNING"

        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.start(ranges=[range])
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)
        long_running_job.wait_for_condition(wait_for_instance_to_run)

        assert 0 != failure_tester.fw.stop(failure_tester.mesos_master)
        long_running_job.stop()
        assert 0 != failure_tester.fw.start(failure_tester.mesos_master)
        long_running_job.wait_for_terminated()
