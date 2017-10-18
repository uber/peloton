class Task(object):
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
