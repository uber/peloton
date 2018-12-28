from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task


class Task(object):
    """
    Task represents a peloton task
    """

    def __init__(self, job, instance_id):
        self.job = job
        self.instance_id = instance_id

    def start(self):
        return self.job.start(tasks=[self])

    def stop(self):
        return self.job.stop(tasks=[self])

    def restart(self):
        return self.job.restart(tasks=[self])

    def get_info(self):
        return self.job.get_task_info(self.instance_id)

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
