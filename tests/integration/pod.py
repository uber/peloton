from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2 as pod

POD_STATE_PREFIX = 'POD_STATE_'


class Pod(object):
    """
    Pod represents a peloton pod for stateless job
    """

    def __init__(self, job, instance_id):
        self.stateless_job = job
        self.instance_id = instance_id

    def get_pod_status(self):
        return self.stateless_job.get_pod_status(self.instance_id)

    @property
    def state_str(self):
        state_name = pod.PodState.Name(self.get_pod_status().state)
        # trim the prefix so pod state is the same as old task state,
        # and the api can be used for both job tests
        return state_name[len(POD_STATE_PREFIX):]
