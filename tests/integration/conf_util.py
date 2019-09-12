from collections import defaultdict
import logging
import os
import string
import random

from google.protobuf import json_format
from job import Job
from peloton_client.pbgen.peloton.api.v0.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from util import load_test_config

#########################################
# Constants
#########################################
# general configs.
CONFIG_FILES = ["test_job.yaml", "test_task.yaml"]
MESOS_MASTER = ["peloton-mesos-master"]
MESOS_AGENTS = [
    "peloton-mesos-agent0",
    "peloton-mesos-agent1",
    "peloton-mesos-agent2",
]
JOB_MGRS = ["peloton-jobmgr0"]
RES_MGRS = ["peloton-resmgr0"]
HOST_MGRS = ["peloton-hostmgr0"]
AURORA_BRIDGE = ["peloton-aurorabridge0"]
PLACEMENT_ENGINES = ["peloton-placement0", "peloton-placement1"]

# job_query related constants.
NUM_JOBS_PER_STATE = 1
TERMINAL_JOB_STATES = ["SUCCEEDED", "RUNNING", "FAILED"]
ACTIVE_JOB_STATES = ["PENDING", "RUNNING", "INITIALIZED"]

# task_query related constants.
TASK_STATES = ["SUCCEEDED", "FAILED", "RUNNING"]
DEFAUILT_TASKS_COUNT = len(TASK_STATES)

HOSTPOOL_DEFAULT = "default"
HOSTPOOL_BATCH_RESERVED = "batch_reserved"
HOSTPOOL_SHARED = "shared"
HOSTPOOL_STATELESS = "stateless"

log = logging.getLogger(__name__)


"""
Creates a JobConfig object.

Arg:
    file_name: Load base config from this file,
        (either 'test_task.yaml' or 'test_job.yaml').
    job_name: string type. The Job name.
    job_owner: string type. The Job owner.
    job_state: string type. Has the value of 'SUCCEEDED', 'RUNNING', or 'FAILED'.
    task_states: a list of tuples, e.g. [<task_state>, <task_count>].
        Needed for 'task_query' config.

Returns:
    job_pb2.JobConfig object is returned.
"""


def generate_job_config(
    file_name, job_name=None, job_owner=None, job_state=None, task_states=None
):
    job_config = _load_job_cfg_proto(file_name)

    if file_name == "test_job.yaml":
        # Create job_config for `job_query`.
        job_config.name = job_name
        job_config.owningTeam = job_owner
        task_cfg = create_task_cfg(job_state)
        job_config.defaultConfig.MergeFrom(task_cfg)
    else:
        assert task_states

        # Create config for `task_query`.
        is_state_mixed = True if len(task_states) > 1 else False
        tasks_count = sum(i[1] for i in task_states)

        if is_state_mixed:
            mixed_tasks_cfg = create_task_configs_by_state(task_states)
            job_config.instanceConfig.MergeFrom(mixed_tasks_cfg)
        else:
            default_cfg = create_task_cfg(task_states[0][0])
            job_config.defaultConfig.MergeFrom(default_cfg)

        job_config.instanceCount = tasks_count
        sla_config = _create_sla_cfg(job_config.sla, tasks_count)
        job_config.sla.MergeFrom(sla_config)

    return job_config


"""
Creates a TaskConfig object.

Arg:
    task_state: a string value of 'SUCCEEDED', 'RUNNING', or 'FAILED'.

Returns:
    a TaskConfig object is returned.
"""


def create_task_cfg(task_state="SUCCEEDED", task_name=None):
    assert task_state in TASK_STATES
    commands = {
        "SUCCEEDED": "echo 'succeeded instance task' & sleep 1",
        "RUNNING": "echo 'running instance task' & sleep 100",
        "FAILED": "echo 'failed instance task' & exit(2)",
    }

    return task.TaskConfig(
        command=mesos.CommandInfo(shell=True, value=commands.get(task_state)),
        name=task_name,
    )


#########################################
# Helper Functions.
#########################################
"""
Loads and returns the JobConfig object based on the input yaml file.
"""


def _load_job_cfg_proto(job_file):
    job_config_dump = load_test_config(job_file)
    job_config = job.JobConfig()
    json_format.ParseDict(job_config_dump, job_config)
    return job_config


"""
Get a map of job objects by state and their common identifier salt

Args:
    _num_jobs_per_state: number of job objects per state.

Returns:
    dict of jobs list per state is returned
"""


def create_job_config_by_state(_num_jobs_per_state=NUM_JOBS_PER_STATE):
    salt = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
    )
    name = "TestJob-" + salt
    owner = "compute-" + salt
    jobs_by_state = defaultdict(list)

    # create three jobs per state with this owner and name
    for state in TERMINAL_JOB_STATES:
        for i in xrange(_num_jobs_per_state):
            job = Job(
                job_config=generate_job_config(
                    file_name="test_job.yaml",
                    # job name will be in format: TestJob-<salt>-<inst-id>-<state>
                    job_name=name + "-" + str(i) + "-" + state,
                    job_owner=owner,
                    job_state=state,
                )
            )
            jobs_by_state[state].append(job)
    return salt, jobs_by_state


"""
Creates an InstanceConfig object from given task_states
for `task query` tests.

Arg:
    task_states: a list of tuples: [(`task_state`, `task_count`)].
    For example, [('SUCCEEDED', 2), ('FAILED', 1)]

Returns:
    a map of task configs by state in a job.
"""


def create_task_configs_by_state(task_states):
    for i in task_states:
        assert isinstance(i, tuple)

    tasks, index = {}, 0
    for state, num in task_states:
        for i in range(index, index + num):
            tasks[i] = create_task_cfg(state, task_name="task-" + str(i))
        index += num

    return tasks


"""
Updates `maximumRunningInstances` with number of tasks created in
SLAConfig object, which is needed for `task query` tests.

Arg:
    curr: Current SlaConfig object read from JobConfig file.
    tasks_count: total number of tasks to be created.
Returns:
    a structured SlaConfig object.
"""


def _create_sla_cfg(curr, tasks_count=DEFAUILT_TASKS_COUNT):
    assert isinstance(curr, job.SlaConfig)

    updated_sla = job.SlaConfig(
        priority=curr.priority,
        preemptible=curr.preemptible,
        maximumRunningInstances=tasks_count,
    )
    return updated_sla


# Returns the type of the minicluster. This will either be "k8s",
# "mesos" or "".
def minicluster_type():
    return os.getenv("MINICLUSTER_TYPE")


# Returns whether host-pools should be used for placement decisions.
def use_host_pool():
    return os.getenv("USE_HOST_POOL", "false").lower() in ["true", "1"]
