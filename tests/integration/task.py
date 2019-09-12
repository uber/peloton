import logging
import time

from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.private.resmgrsvc import (
    resmgrsvc_pb2 as resmgr,
)

logging.basicConfig()
log = logging.getLogger(__name__)


class Task(object):
    """
    Task represents a peloton task
    """

    def __init__(self, job, instance_id):
        self.job = job
        self.instance_id = instance_id

    @property
    def mesos_task_id(self):
        return self.get_info().runtime.mesosTaskId.value

    @property
    def goal_state(self):
        return self.get_info().runtime.goalState

    @property
    def state(self):
        return self.get_info().runtime.state

    @property
    def state_str(self):
        return task.TaskState.Name(self.state)

    def start(self):
        return self.job.start(ranges=self._get_range())

    def stop(self):
        return self.job.stop(ranges=self._get_range())

    def restart(self):
        return self.job.restart(ranges=self._get_range())

    def get_runtime(self):
        return self.get_info().runtime

    def get_info(self):
        return self.job.get_task_info(self.instance_id)

    def wait_for_pending_state(self, goal_state):
        """
        Waits for the task to reach a particular pending state in the resource
        manager Eg READY,PLACING,PLACED etc
        :return:
        """
        state = ""
        attempts = 0
        start = time.time()
        log.info("%s waiting for state %s", self.mesos_task_id, goal_state)
        state_transition_failure = True
        while attempts < self.job.config.max_retry_attempts:
            try:
                resp = self._get_active_tasks()
                new_state = self._get_state(resp)
                if state != new_state:
                    log.info(
                        "%s transitioned to state %s",
                        self.mesos_task_id,
                        new_state,
                    )
                state = new_state
                if state == goal_state:
                    state_transition_failure = False
                    break
            except Exception as e:
                log.warn(e)
            finally:
                time.sleep(self.job.config.sleep_time_sec)
                attempts += 1

        if state_transition_failure:
            log.info(
                "%s goal_state:%s current_state:%s attempts: %s",
                self.mesos_task_id,
                goal_state,
                state,
                str(attempts),
            )
            assert False

        if attempts == self.job.config.max_retry_attempts:
            log.info(
                "%s max attempts reached to wait for goal state",
                self.mesos_task_id,
            )
            log.info(
                "%s goal_state:%s current_state:%s",
                self.mesos_task_id,
                goal_state,
                state,
            )
            assert False

        end = time.time()
        elapsed = end - start
        log.info(
            "%s state transition took %s seconds", self.mesos_task_id, elapsed
        )
        assert state == goal_state

    def _get_state(self, tasks_by_state):
        """
        returns the state of this task
        :param tasks_by_state: dict of states to a list of tasks in that state
        :return: the state of this task, otherwise empty
        """
        for state, entries in tasks_by_state.iteritems():
            for e in entries.taskEntry:
                if e.taskID == self.mesos_task_id:
                    return state
        return ""

    def _get_active_tasks(self):
        request = resmgr.GetActiveTasksRequest(jobID=self.job.job_id)
        resp = self.job.client.resmgr_svc.GetActiveTasks(
            request,
            metadata=self.job.client.resmgr_metadata,
            timeout=self.job.config.rpc_timeout_sec,
        )

        return resp.tasksByState

    def _get_range(self):
        _range = task.InstanceRange(to=self.instance_id + 1)
        # 'from' a reserved keyword so we have to do this
        setattr(_range, "from", self.instance_id)
        return [_range]
