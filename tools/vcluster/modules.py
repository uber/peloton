import time

from task_config import (
    create_task_config,
)

from color_print import (
    print_okblue,
)


class Module(object):
    def __init__(self, module_name, label_name, peloton_helper):
        """
        param module_name: the name of the job
        param label_name: the label of the job
        param peloton_helper: instance of PelotonClientHelper

        type module_name: str
        type label_name: str
        type peloton_helper: PelotonClientHelper

        """
        self.name = module_name
        self.label = label_name

        self.peloton_helper = peloton_helper
        self.job_id = ''

    def setup(self, dynamic_env, instance_number, job_name=None):
        """
        param dynamic: dict of dynamic environment virable
        param instance_number: number of tasks in the job

        type dynamic: dict
        type instance_number: int

        rtype: job.CreateResponse
        """
        if not job_name:
            job_name = self.label + '_' + self.name
        task_config = create_task_config(self.name, dynamic_env)
        resp = self.peloton_helper.create_job(
            label=self.label,
            name=job_name,
            default_task_config=task_config,
            num_instance=instance_number,
        )
        self.job_id = resp.jobId.value
        print_okblue('Waiting for job %s creating...' % job_name)
        time.sleep(15)

    def teardown(self, job_name=None):
        """
        param job_name: name of the job so specify
        type job_name: str
        """
        if not job_name:
            job_name = self.label + '_' + self.name
        ids = self.peloton_helper.get_jobs_by_label(
            self.label, job_name, ['RUNNING']
        )
        for id in ids:
            self.peloton_helper.stop_job(id)
        print_okblue('Waiting for job %s teardown...' % job_name)
        time.sleep(15)


class Zookeeper(Module):
    def __init__(self, label_name, peloton_helper):
        """
        type param label_name: str
        type peloton_helper: PelotonClientHelper
        """
        super(Zookeeper, self).__init__(
            'zookeeper', label_name, peloton_helper
        )

    def get_host_port(self):
        """
        rtype: host, port: str, str
        """
        zk_tasks = self.peloton_helper.get_tasks(self.job_id)
        host = zk_tasks[0].runtime.host
        port = zk_tasks[0].runtime.ports['ZOO_PORT']
        return host, port


class MesosMaster(Module):
    def __init__(self, label_name, peloton_helper):
        """
        type param label_name: str
        type peloton_helper: PelotonClientHelper
        """
        super(MesosMaster, self).__init__(
            'mesos-master', label_name, peloton_helper
        )


class MesosSlave(Module):
    def __init__(self, label_name, peloton_helper):
        """
        type param label_name: str
        type peloton_helper: PelotonClientHelper
        """
        super(MesosSlave, self).__init__(
            'mesos-slave', label_name, peloton_helper
        )

    def setup(self, dynamic_env, instance_number, job_name=None):
        """
        param dynamic: dict of dynamic environment virable
        param instance_number: number of tasks in the job

        type dynamic: dict
        type instance_number: int

        rtype: job.CreateResponse
        """
        if not job_name:
            job_name = self.label + '_' + self.name

        instance_config = {}

        for i in range(instance_number):
            dynamic_env['MESOS_HOSTNAME'] = '_'.join(
                [self.label, self.name, str(i)]
            )
            instance_config.update(
                {i: create_task_config(self.name, dynamic_env)}
            )

        resp = self.peloton_helper.create_job(
            label=self.label,
            name=job_name,
            default_task_config=instance_config[0],
            instance_config=instance_config,
            num_instance=instance_number,
        )
        self.job_id = resp.jobId.value
        print_okblue('Waiting for job %s setup...' % job_name)
        time.sleep(15)


class Cassandra(Module):
    def __init__(self, label_name, peloton_helper):
        """
        type param label_name: str
        type peloton_helper: PelotonClientHelper
        """
        super(Cassandra, self).__init__(
            'cassandra', label_name, peloton_helper
        )


class Peloton(Module):
    def __init__(self, label_name, peloton_helper):
        """
        type param label_name: str
        type peloton_helper: PelotonClientHelper
        """
        super(Peloton, self).__init__(
            'peloton', label_name, peloton_helper
        )
