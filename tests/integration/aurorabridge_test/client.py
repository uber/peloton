import os

import requests
import thriftrw


api = thriftrw.install(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    '../../../aurorabridge/thrift/api.thrift'))

AuroraSchedulerManager = api.AuroraSchedulerManager
ReadOnlyScheduler = api.ReadOnlyScheduler


class Client(object):

    def __init__(self, addr='localhost:8082'):
        self.addr = addr

    def get_job_update_summaries(self, *args):
        return self._send(
            ReadOnlyScheduler,
            ReadOnlyScheduler.getJobUpdateSummaries,
            *args)

    def get_jobs(self, *args):
        return self._send(
            ReadOnlyScheduler,
            ReadOnlyScheduler.getJobs,
            *args)

    def get_tasks_without_configs(self, *args):
        return self._send(
            ReadOnlyScheduler,
            ReadOnlyScheduler.getTasksWithoutConfigs,
            *args)

    def kill_tasks(self, *args):
        return self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.killTasks,
            *args)

    def start_job_update(self, *args):
        return self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.startJobUpdate,
            *args)

    def _send(self, service, method, *args):
        req = method.request(*args)
        res = requests.post(
            'http://%s/api' % self.addr,
            headers={
                'Rpc-Caller': 'aurorabridge-test-client',
                'Rpc-Encoding': 'thrift',
                'Rpc-Service': 'peloton-aurorabridge',
                'Rpc-Procedure': '%s::%s' % (service.__name__, method.name),
                'Context-TTL-MS': '10000',
            },
            data=api.dumps(req))
        if res.status_code != 200:
            raise Exception('{url} {method}: {code} {reason}: {body}'.format(
                url=res.url,
                method=method.name,
                code=res.status_code,
                reason=res.reason,
                body=res.text))
        return api.loads(method.response, res.content).success
