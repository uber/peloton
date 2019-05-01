import os

import requests
import thriftrw


api = thriftrw.install(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../../pkg/aurorabridge/thrift/api.thrift",
    )
)

AuroraSchedulerManager = api.AuroraSchedulerManager
ReadOnlyScheduler = api.ReadOnlyScheduler


class ResponseError(Exception):
    """
    Raised whenever there is a non-OK response code.
    """

    def __init__(self, response):
        self.code = response.responseCode
        self.msg = ",".join(map(lambda d: d.message, response.details))

    def __str__(self):
        return "bad response: {code} {msg}".format(
            code=api.ResponseCode.name_of(self.code), msg=self.msg
        )


class Client(object):
    def __init__(self, addr="localhost:5396"):
        self.addr = addr

    def get_job_update_summaries(self, *args):
        res = self._send(
            ReadOnlyScheduler, ReadOnlyScheduler.getJobUpdateSummaries, *args
        )
        return res.result.getJobUpdateSummariesResult

    def get_job_update_details(self, *args):
        res = self._send(
            ReadOnlyScheduler, ReadOnlyScheduler.getJobUpdateDetails, *args
        )
        return res.result.getJobUpdateDetailsResult

    def get_job_summary(self, *args):
        res = self._send(
            ReadOnlyScheduler, ReadOnlyScheduler.getJobSummary, *args
        )
        return res.result.jobSummaryResult

    def get_jobs(self, *args):
        res = self._send(ReadOnlyScheduler, ReadOnlyScheduler.getJobs, *args)
        return res.result.getJobsResult

    def get_tasks_without_configs(self, *args):
        res = self._send(
            ReadOnlyScheduler, ReadOnlyScheduler.getTasksWithoutConfigs, *args
        )
        return res.result.scheduleStatusResult

    def get_config_summary(self, *args):
        res = self._send(
            ReadOnlyScheduler, ReadOnlyScheduler.getConfigSummary, *args
        )
        return res.result.configSummaryResult

    def kill_tasks(self, *args):
        self._send(
            AuroraSchedulerManager, AuroraSchedulerManager.killTasks, *args
        )
        # killTasks has no result.

    def pulse_job_update(self, *args):
        res = self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.pulseJobUpdate,
            *args
        )
        return res.result.pulseJobUpdateResult

    def start_job_update(self, *args):
        res = self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.startJobUpdate,
            *args
        )
        return res.result.startJobUpdateResult

    def abort_job_update(self, *args):
        self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.abortJobUpdate,
            *args
        )
        # abortJobUpdate has no result

    def pause_job_update(self, *args):
        self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.pauseJobUpdate,
            *args
        )
        # pauseJobUpdate has no result

    def rollback_job_update(self, *args):
        self._send(
            AuroraSchedulerManager,
            AuroraSchedulerManager.rollbackJobUpdate,
            *args
        )
        # rollbackJobUpdate has no result

    def _send(self, service, method, *args):
        req = method.request(*args)
        res = requests.post(
            "http://%s/api" % self.addr,
            headers={
                "Rpc-Caller": "aurorabridge-test-client",
                "Rpc-Encoding": "thrift",
                "Rpc-Service": "peloton-aurorabridge",
                "Rpc-Procedure": "%s::%s" % (service.__name__, method.name),
                "Context-TTL-MS": "30000",
            },
            data=api.dumps(req),
        )
        if res.status_code != 200:
            raise Exception(
                "{url} {method}: {code} {reason}: {body}".format(
                    url=res.url,
                    method=method.name,
                    code=res.status_code,
                    reason=res.reason,
                    body=res.text,
                )
            )

        response = api.loads(method.response, res.content).success
        if response.responseCode != api.ResponseCode.OK:
            raise ResponseError(response)
        return response
