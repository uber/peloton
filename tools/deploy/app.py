from __future__ import absolute_import

import base64
import os
import os.path

from enum import Enum
from time import sleep, time

from aurora.api.constants import AURORA_EXECUTOR_NAME, ACTIVE_STATES
from aurora.api.ttypes import (
    JobKey,
    TaskConfig,
    JobConfiguration,
    ExecutorConfig,
    Container,
    DockerContainer,
    DockerParameter,
    Identity,
    ResponseCode,
    ScheduleStatus,
    TaskQuery,
    JobUpdateRequest,
    JobUpdateSettings,
    JobUpdateQuery,
    JobUpdateStatus,
    Range,
    Constraint,
    TaskConstraint,
    LimitConstraint,
    Resource,
    Metadata,
)
from aurora.schema.thermos.schema_base import (
    Task as ThermosTask,
    Process as ThermosProcess,
    Resources,
    Logger as ThermosLogger,
    LoggerMode,
    RotatePolicy,
)
from aurora.schema.aurora.base import (
    Announcer,
    MesosJob as ThermosJob,
    HealthCheckConfig,
    HealthCheckerConfig,
    HttpHealthChecker,
    DisableLifecycle,
)

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

AURORA_ROLE = "peloton"
AURORA_ENVIRONMENT = "production"
AURORA_USER = "peloton"


MAX_WAIT_TIME_SECONDS = 600
WAIT_INTERVAL_SECONDS = 10


def combine_messages(response):
    """
    Combines the message found in the details of a response.

    :param response: response to extract messages from.
    :return: Messages from the details in the response, or an empty string if
        there were no messages.
    """
    return ", ".join(
        [d.message or "Unknown error" for d in (response.details or [])]
    )


class Role(Enum):
    """
    The role of a Peloton app instance
    """

    UNKNOWN = 1
    LEADER = 2
    FOLLOWER = 3
    ALL = 4


class Instance(object):
    """
    Representation of an instance of a Peloton application
    """

    def __init__(self, task, role):
        assert task.assignedTask
        self.instance_id = task.assignedTask.instanceId
        self.host = task.assignedTask.slaveHost
        self.state = task.status

        # TODO: query Peloton app endpoint to determine leader/follower role
        self.role = role


class App(object):
    """
    Representation of a Peloton application with multiple instances
    """

    def __init__(self, **kwargs):
        """
        Initializes a Peloton application
        """
        # Default attributes
        self.enable_debug_logging = False
        self.enable_secrets = False
        self.cpu_limit = 4.0
        self.mem_limit = 16 * GB
        self.disk_limit = 16 * GB
        self.scarce_resource_types = None
        self.slack_resource_types = None
        self.enable_revocable_resources = None
        self.bin_packing = None
        self.qos_advisor_service_address = None
        self.stream_only_mode = True
        self.task_preemption_period = "60s"
        self.enable_sla_tracking = False
        self.enable_preemption = False
        self.respool_path = None
        self.gpu_respool_path = None
        self.auth_type = "NOOP"
        self.auth_config_file = ""
        self.enable_inplace_update = False
        self.use_host_pool = False
        self.http_port = None
        self.grpc_port = None

        for k, v in kwargs.iteritems():
            setattr(self, k, v)

        self.client = self.cluster.client

        self.prod_yaml_location = self.cluster.cfg_file

        if self.num_instances < 1:
            raise Exception("App %s has no instances" % self.name)

        self.job_key = JobKey(
            role=AURORA_ROLE, environment=AURORA_ENVIRONMENT, name=self.name
        )

        # Generate the new job config for this app
        self.desired_job_config = self.get_desired_job_config()

        # Save current job config so that we can rollback to later
        self.current_job_config = self.get_current_job_config()

    def get_docker_params(self):
        """
        Returns the docker params for a given Peloton application
        """
        mesos_zk_path = "zk://%s/%s" % (
            ",".join(self.cluster.zookeeper),
            self.cluster.mesos_zk_path,
        )
        peloton_zk_endpoints = "\n".join(self.cluster.zookeeper)

        # add common variables
        env_vars = {
            "ENVIRONMENT": "production",
            "CONFIG_DIR": "./config",
            "APP": self.name,
            # TODO: fix Peloton code to only take self.cluster.mesos_zk_path
            "MESOS_ZK_PATH": mesos_zk_path,
            "ENABLE_DEBUG_LOGGING": self.enable_debug_logging,
            "ELECTION_ZK_SERVERS": peloton_zk_endpoints,
            "USE_CASSANDRA": self.cluster.cassandra_contact_points is not None,
            "CASSANDRA_HOSTS": "\n".join(
                self.cluster.cassandra_contact_points
            ),
            "CASSANDRA_STORE": self.cluster.cassandra_keyspace,
            "CASSANDRA_PORT": self.cluster.cassandra_port,
            "CLUSTER": self.cluster.name,
            "DATACENTER": getattr(self.cluster, "datacenter", ""),
            "MESOS_AGENT_WORK_DIR": self.cluster.mesos_agent_work_dir,
            "AUTO_MIGRATE": self.cluster.auto_migrate,
            "ENABLE_SENTRY_LOGGING": self.cluster.enable_sentry_logging,
            "SECRET_CONFIG_DIR": getattr(
                self.cluster, "secret_config_dir", ""
            ),
            "MESOS_SECRET_FILE": getattr(
                self.cluster, "mesos_secret_file", ""
            ),
            "PELOTON_SECRET_FILE": getattr(
                self.cluster, "peloton_secret_file", ""
            ),
            "ENABLE_SECRETS": self.enable_secrets,
            "AUTH_TYPE": self.auth_type,
            "AUTH_CONFIG_FILE": self.auth_config_file,
        }

        self.add_app_specific_vars(env_vars)

        name = self.name
        if name.startswith("placement_"):
            name = "placement"

        # base64 encode the prod config and add it to the PRODUCTION_CONFIG
        # environment variable inside the container
        prod_config_path = self.get_app_path().format(name)
        with open(prod_config_path, "rb") as config_file:
            env_vars["PRODUCTION_CONFIG"] = base64.b64encode(
                config_file.read()
            )

        params = [
            DockerParameter(name="env", value="%s=%s" % (key, val))
            for key, val in env_vars.iteritems()
        ]

        volumes = [("/var/log/peloton", "/var/log/peloton", "rw")]

        # Mount langley secrets if secret dir is specified.
        if getattr(self.cluster, "secret_config_dir", ""):
            volumes.append(
                (
                    self.cluster.secret_config_dir,
                    self.cluster.secret_config_dir,
                    "ro",
                )
            )

        params.extend(
            DockerParameter(name="volume", value="%s:%s:%s" % (src, dst, mode))
            for src, dst, mode in volumes
        )

        return params

    def add_app_specific_vars(self, env_vars):
        """
        Adds env variables specific to the application
        """
        if self.name == "placement_stateful":
            env_vars["TASK_TYPE"] = "STATEFUL"
            env_vars["APP"] = "placement"
            env_vars["APP_TYPE"] = "placement_stateful"
            if self.cluster.use_host_pool:
                env_vars["USE_HOST_POOL"] = self.cluster.use_host_pool

        if self.name == "placement_stateless":
            env_vars["TASK_TYPE"] = "STATELESS"
            env_vars["APP"] = "placement"
            env_vars["APP_TYPE"] = "placement_stateless"
            if self.cluster.use_host_pool:
                env_vars["USE_HOST_POOL"] = self.cluster.use_host_pool

        if self.name == "placement":
            if getattr(self, "task_dequeue_limit", ""):
                env_vars["TASK_DEQUEUE_LIMIT"] = self.task_dequeue_limit
            if getattr(self, "task_dequeue_period", ""):
                env_vars["TASK_DEQUEUE_PERIOD"] = self.task_dequeue_period
            if self.cluster.use_host_pool:
                env_vars["USE_HOST_POOL"] = self.cluster.use_host_pool

        if self.name == "jobmgr":
            env_vars["JOB_TYPE"] = getattr(self, "job_type", "BATCH")
            env_vars["JOB_RUNTIME_CALCULATION_VIA_CACHE"] = getattr(
                self, "job_runtime_calculation_via_cache", False
            )
            env_vars["TASK_KILL_RATE_LIMIT"] = getattr(
                self, "task_kill_rate_limit", 0.0
            )
            env_vars["TASK_KILL_BURST_LIMIT"] = getattr(
                self, "task_kill_burst_limit", 0
            )
            env_vars["TASK_LAUNCH_TIMEOUT"] = getattr(
                self, "task_launch_timeout", "0"
            )
            env_vars["TASK_START_TIMEOUT"] = getattr(
                self, "task_start_timeout", "0"
            )
            env_vars["EXECUTOR_SHUTDOWN_RATE_LIMIT"] = getattr(
                self, "executor_shutdown_rate_limit", 0.0
            )
            env_vars["EXECUTOR_SHUTDOWN_BURST_LIMIT"] = getattr(
                self, "executor_shutdown_burst_limit", 0
            )

        if self.name == "archiver":
            env_vars["ENABLE_ARCHIVER"] = self.enable_archiver
            env_vars["STREAM_ONLY_MODE"] = self.stream_only_mode
            env_vars["POD_EVENTS_CLEANUP"] = self.pod_events_cleanup
            env_vars["ARCHIVE_AGE"] = self.archive_age
            env_vars["ARCHIVE_INTERVAL"] = self.archive_interval
            env_vars["ARCHIVE_STEP_SIZE"] = self.archive_step_size
            env_vars["KAFKA_TOPIC"] = self.kafka_topic

        if self.name == "resmgr":
            env_vars["TASK_PREEMPTION_PERIOD"] = getattr(
                self, "task_preemption_period", "60s"
            )
            env_vars["ENABLE_SLA_TRACKING"] = getattr(
                self, "enable_sla_tracking", False
            )
            env_vars["ENABLE_PREEMPTION"] = getattr(
                self, "enable_preemption", False
            )

        if self.name == "hostmgr":
            if self.scarce_resource_types:
                env_vars["SCARCE_RESOURCE_TYPES"] = ",".join(
                    self.scarce_resource_types
                )
            if self.slack_resource_types:
                env_vars["SLACK_RESOURCE_TYPES"] = ",".join(
                    self.slack_resource_types
                )
            if self.enable_revocable_resources:
                env_vars[
                    "ENABLE_REVOCABLE_RESOURCES"
                ] = self.enable_revocable_resources
            if self.bin_packing:
                env_vars["BIN_PACKING"] = self.bin_packing
            if self.qos_advisor_service_address:
                env_vars["QOS_ADVISOR_SERVICE_ADDRESS"] \
                    = self.qos_advisor_service_address
            if self.cluster.use_host_pool:
                env_vars["ENABLE_HOST_POOL"] = self.cluster.use_host_pool

        if self.name == "aurorabridge":
            if self.respool_path:
                env_vars["RESPOOL_PATH"] = self.respool_path
            if self.gpu_respool_path:
                env_vars["GPU_RESPOOL_PATH"] = self.gpu_respool_path
            if self.enable_inplace_update:
                env_vars["ENABLE_INPLACE_UPDATE"] = "true"

    def get_app_path(self):
        """
        Returns the formatted path for app config
        """
        dirname = os.path.dirname(self.prod_yaml_location)
        path = os.path.join(
            dirname, "../..", "config", "{}", "production.yaml"
        )
        return path

    def get_docker_image(self):
        """
        Returns the docker image path for a Peloton app
        """
        return "%s/%s:%s" % (
            self.cluster.docker_registry,
            self.cluster.docker_repository,
            self.cluster.version,
        )

    def get_executor_config(self):
        """
        Returns the Thermos executor config for a Peloton app
        """

        host_logdir = "/var/log/peloton/%s" % self.name
        sandbox_logdir = "$MESOS_DIRECTORY/sandbox/.logs/%s/0" % self.name
        cmdline = " && ".join(
            [
                "rm -rf %s" % host_logdir,
                "ln -s %s %s" % (sandbox_logdir, host_logdir),
                "/bin/entrypoint.sh",
            ]
        )
        # Override executor command for api server to inject
        # ports environment variable
        if self.name == "apiserver" and \
                self.http_port is None and \
                self.grpc_port is None:
            cmdline = " && ".join(
                [
                    "rm -rf %s" % host_logdir,
                    "ln -s %s %s" % (sandbox_logdir, host_logdir),
                    "exec env HTTP_PORT={{thermos.ports[http]}} "
                    "GRPC_PORT={{thermos.ports[grpc]}} "
                    "/bin/entrypoint.sh",
                ]
            )
        entrypoint_process = ThermosProcess(
            name=self.name,
            cmdline=cmdline,
            logger=ThermosLogger(
                mode=LoggerMode("rotate"),
                rotate=RotatePolicy(log_size=1 * GB, backups=10),
            ),
        )
        thermos_task = ThermosTask(
            name=self.name,
            processes=[entrypoint_process],
            resources=Resources(
                cpu=self.cpu_limit, ram=self.mem_limit, disk=self.disk_limit
            ),
        )
        health_check_config = HealthCheckConfig(
            health_checker=HealthCheckerConfig(
                http=HttpHealthChecker(
                    endpoint="/health",
                    expected_response="OK",
                    expected_response_code=200,
                )
            ),
            initial_interval_secs=15,
            interval_secs=10,
            max_consecutive_failures=4,
            timeout_secs=1,
        )
        announce = Announcer()
        if self.http_port is not None:
            announce = Announcer(portmap={"health": self.http_port})
        elif self.name == "apiserver":
            # Use assigned http port for health check if it is api server
            # and static http port is not configured
            announce = Announcer(portmap={"health": "{{thermos.ports[http]}}"})
        thermos_job = ThermosJob(
            name=self.name,
            role=AURORA_ROLE,
            cluster=self.cluster.name,
            environment=AURORA_ENVIRONMENT,
            task=thermos_task,
            production=False,
            service=True,
            health_check_config=health_check_config,
            announce=announce,
        )
        executor_config = ExecutorConfig(
            name=AURORA_EXECUTOR_NAME, data=thermos_job.json_dumps()
        )

        return executor_config

    def get_desired_job_config(self):
        """
        Return the Aurora job configuration defined in Thrift API so that
        we can create a job via Aurora API.
        """

        # Add docker container
        container = Container(
            mesos=None,
            docker=DockerContainer(
                image=self.get_docker_image(),
                parameters=self.get_docker_params(),
            ),
        )

        host_limit = Constraint(
            name=self.cluster.constraint,
            constraint=TaskConstraint(limit=LimitConstraint(limit=1)),
        )

        # Set task metadata if presents in config
        if hasattr(self, 'metadata'):
            taskMetaData = set(
                [Metadata(key=k, value=v) for k, v in self.metadata.items()])
        else:
            taskMetaData = set()

        # Set task resources
        resources = set(
            [
                Resource(numCpus=self.cpu_limit),
                Resource(ramMb=self.mem_limit / MB),
                Resource(diskMb=self.disk_limit / MB),
            ]
        )
        if self.name == "apiserver":
            if self.http_port is None:
                resources.add(Resource(namedPort="http"))
            if self.grpc_port is None:
                resources.add(Resource(namedPort="grpc"))

        task_config = TaskConfig(
            job=self.job_key,
            owner=Identity(user=AURORA_USER),
            isService=True,
            numCpus=self.cpu_limit,
            ramMb=self.mem_limit / MB,
            diskMb=self.disk_limit / MB,
            priority=0,
            maxTaskFailures=0,
            production=False,
            tier="preemptible",
            resources=resources,
            contactEmail="peloton-oncall-group@uber.com",
            executorConfig=self.get_executor_config(),
            container=container,
            constraints=set([host_limit]),
            requestedPorts=set(),
            mesosFetcherUris=set(),
            taskLinks={},
            metadata=taskMetaData,
        )

        job_config = JobConfiguration(
            key=self.job_key,
            owner=Identity(user=AURORA_USER),
            taskConfig=task_config,
            instanceCount=self.num_instances,
        )
        return job_config

    def get_current_job_config(self):
        """
        Return the current job config by querying the Aurora API
        """
        resp = self.client.getJobSummary(AURORA_ROLE)
        if resp.responseCode != ResponseCode.OK:
            raise Exception(combine_messages(resp))

        job_config = None
        for s in resp.result.jobSummaryResult.summaries:
            if s.job.key == self.job_key:
                job_config = s.job
                break

        if job_config:
            instances = self.get_instances()
            job_config.instanceCount = len(instances.get(Role.ALL, []))

        return job_config

    def get_instances(self):
        """
        Returns all instances grouped by role if exist by querying Aurora API
        """

        # Setup task query for task status of the Aurora job
        task_query = TaskQuery(
            role=AURORA_ROLE,
            environment=AURORA_ENVIRONMENT,
            jobName=self.name,
            statuses=ACTIVE_STATES,
        )

        resp = self.client.getTasksWithoutConfigs(task_query)
        if resp.responseCode != ResponseCode.OK:
            raise Exception(combine_messages(resp))

        instances = {}
        leader = True
        for t in resp.result.scheduleStatusResult.tasks:
            if t.status not in ACTIVE_STATES:
                # Ignore tasks that are not in active states
                continue

            # Temporarily hack to set leader/follower roles
            # TODO: query Peloton app endpoint to find role
            role = Role.LEADER if leader else Role.FOLLOWER
            if leader:
                leader = False

            inst = Instance(t, role)
            instances.setdefault(inst.role, []).append(inst)
            instances.setdefault(Role.ALL, []).append(inst)

        return instances

    def wait_for_running(self, role):
        """
        Wait for the app instances of a given role running
        """

        num_instances = {
            Role.LEADER: 1,
            Role.FOLLOWER: self.num_instances - 1,
            Role.ALL: self.num_instances,
        }[role]

        start_time = time()
        while time() < start_time + MAX_WAIT_TIME_SECONDS:
            instances = self.get_instances().get(role, [])
            all_running = True

            for i in instances:
                if i.state != ScheduleStatus.RUNNING:
                    all_running = False
                    break

            print(
                "Wait for %s %s instances running: %d / %d"
                % (self.name, role.name, all_running, len(instances))
            )

            if all_running and len(instances) == num_instances:
                return True

            sleep(WAIT_INTERVAL_SECONDS)

        return False

    def update_instances(self, instances, job_config):
        """
        Update the task config of the given app instances
        """

        instance_ids = [i.instance_id for i in instances]

        req = JobUpdateRequest(
            taskConfig=job_config.taskConfig,
            instanceCount=self.num_instances,
            settings=JobUpdateSettings(
                updateGroupSize=1, maxPerInstanceFailures=3
            ),
        )
        if instance_ids:
            req.settings.updateOnlyTheseInstances = set(
                Range(i, i) for i in instance_ids
            )

        resp = self.client.startJobUpdate(
            req, "Update %s instances for %s" % (len(instances), self.name)
        )
        if resp.responseCode == ResponseCode.INVALID_REQUEST:
            if resp.result is None:
                raise Exception(combine_messages(resp))

            update_key = resp.result.startJobUpdateResult.key
            update_summary = resp.result.startJobUpdateResult.updateSummary
            status = update_summary.state.status
            if status == JobUpdateStatus.ROLLING_FORWARD:
                # Abort the current update
                print(
                    "Aborting the update for %s (id=%s)"
                    % (self.name, update_key.id)
                )
                self.client.abortJobUpdate(
                    update_key, "Abort by a new deploy session"
                )
                self.wait_for_update_done(update_key)

                # Restart the job update
                resp = self.client.startJobUpdate(
                    req,
                    "Update %s instances for %s" % (len(instances), self.name),
                )
            else:
                raise Exception(
                    "Invalid Request for job update (status=%s)"
                    % (status, JobUpdateStatus._VALUES_TO_NAMES[status])
                )

        if resp.responseCode != ResponseCode.OK:
            raise Exception(combine_messages(resp))

        if resp.result is None:
            # No change for the job update
            print(resp.details[0].message)
            return True

        update_key = resp.result.startJobUpdateResult.key
        return self.wait_for_update_done(update_key, instance_ids)

    def wait_for_update_done(self, update_key, instance_ids=[]):
        """
        Wait for the job update to finish
        """

        query = JobUpdateQuery(
            role=AURORA_ROLE, key=update_key, jobKey=self.job_key
        )

        start_time = time()
        while time() < start_time + MAX_WAIT_TIME_SECONDS:
            resp = self.client.getJobUpdateSummaries(query)
            if resp.responseCode != ResponseCode.OK:
                print(combine_messages(resp))
                sleep(WAIT_INTERVAL_SECONDS)
                continue

            result = resp.result.getJobUpdateSummariesResult

            if len(result.updateSummaries) != 1:
                raise Exception(
                    "Got multiple update summaries: %s"
                    % str(result.updateSummaries)
                )

            if result.updateSummaries[0].key != update_key:
                raise Exception(
                    "Mismatch update key, expect %s, received %s"
                    % (update_key, result.updateSummaries[0].key)
                )

            status = result.updateSummaries[0].state.status
            print(
                "Updating %s instances %s (status=%s)"
                % (
                    self.name,
                    instance_ids,
                    JobUpdateStatus._VALUES_TO_NAMES[status],
                )
            )

            if status == JobUpdateStatus.ROLLED_FORWARD:
                return True
            elif status in [
                JobUpdateStatus.ROLLED_BACK,
                JobUpdateStatus.ABORTED,
                JobUpdateStatus.ERROR,
                JobUpdateStatus.FAILED,
            ]:
                return False
            else:
                # Wait for 5 seconds
                sleep(WAIT_INTERVAL_SECONDS)

        return False

    def update_or_create_job(self, callback):
        """
        Update the current job for a Peloton app. Create a new job if the job
        does not exist yet.
        """

        # TODO: find the leader/follower role of each app instance

        if self.current_job_config is None:
            # Create the new Job in Aurora and check the response code
            print(
                "Creating new job for %s with %s instances"
                % (self.name, self.num_instances)
            )

            resp = self.client.createJob(self.desired_job_config)
            if resp.responseCode != ResponseCode.OK:
                raise Exception(combine_messages(resp))

            # Wait for all instances are running
            retval = self.wait_for_running(Role.ALL)

            if retval:
                callback(self)

            return retval

        # Get current leader and follower instances
        cur_instances = self.get_instances()

        # First updade all existing instances, followers first then leader
        for role in [Role.FOLLOWER, Role.LEADER]:
            instances = cur_instances.get(role, [])

            if role == Role.LEADER and len(instances) > 1:
                raise Exception(
                    "Found %d leaders for %s" % (len(instances), self.name)
                )

            if len(instances) == 0:
                print("No %s %s instances to update" % (self.name, role.name))
                continue

            print(
                "Start updating %d %s %s instances"
                % (len(instances), self.name, role.name)
            )

            retval = self.update_instances(instances, self.desired_job_config)

            print(
                "Finish updating %d %s %s instances -- %s"
                % (
                    len(instances),
                    self.name,
                    role.name,
                    "SUCCEED" if retval else "FAILED",
                )
            )

            if not retval or not callback(self):
                # Rollback the update by the caller
                return False

        # Then add any missing instances if needed
        cur_total = len(cur_instances.get(role.ALL, []))
        new_total = self.num_instances > cur_total

        if new_total > 0:
            print("Start adding %d new %s instances" % (new_total, self.name))
            retval = self.update_instances([], self.desired_job_config)
            print(
                "Finish adding %d new %s instances -- %s"
                % (new_total, self.name, "SUCCEED" if retval else "FAILED")
            )

            if not retval or not callback(self):
                # Rollback the update by the caller
                return False

        return True

    def rollback_job(self):
        """
        Rollback the job config of a Peloton app in case of failures
        """
        if self.current_job_config is None:
            # Nothing to do if the job doesn't exist before
            return

        self.update_instances([], self.current_job_config)


class CronApp(App):
    """
    Representation of a CRON job
    """

    def __init__(self, **kwargs):
        """
        Initializes Cron job
        """
        # Default attributes
        self.enable_debug_logging = False
        self.enable_secrets = False
        self.working_dir = None
        self.cmdline = None
        self.cron_schedule = "\*/5 * * * *"
        self.num_instances = 1
        super(CronApp, self).__init__(**kwargs)
        self.cpu_limit = 1.0
        self.mem_limit = 1 * GB
        self.disk_limit = 16 * GB

    def get_executor_config(self):
        """
        Returns the Thermos executor config for a Peloton app
        """
        if self.working_dir is not None:
            cmd = " && ".join(["cd " + self.working_dir, self.cmdline])
        else:
            cmd = self.cmdline

        entrypoint_process = ThermosProcess(name=self.name, cmdline=cmd)
        thermos_task = ThermosTask(
            name=self.name,
            processes=[entrypoint_process],
            resources=Resources(
                cpu=self.cpu_limit, ram=self.mem_limit, disk=self.disk_limit
            ),
        )
        thermos_job = ThermosJob(
            name=self.name,
            role=AURORA_ROLE,
            cluster=self.cluster.name,
            environment=AURORA_ENVIRONMENT,
            task=thermos_task,
            production=False,
            lifecycle=DisableLifecycle,
            cron_schedule=self.cron_schedule[1:],
        )
        executor_config = ExecutorConfig(
            name=AURORA_EXECUTOR_NAME, data=thermos_job.json_dumps()
        )
        return executor_config

    def get_desired_job_config(self):
        """
        Return the Aurora job configuration defined in Thrift API so that
        we can create a job via Aurora API.
        """

        # Add docker container
        container = Container(
            mesos=None,
            docker=DockerContainer(
                image=self.get_docker_image(),
                parameters=self.get_docker_params(),
            ),
        )

        host_limit = Constraint(
            name=self.cluster.constraint,
            constraint=TaskConstraint(limit=LimitConstraint(limit=1)),
        )

        task_config = TaskConfig(
            job=self.job_key,
            owner=Identity(user=AURORA_USER),
            isService=False,
            numCpus=self.cpu_limit,
            ramMb=self.mem_limit / MB,
            diskMb=self.disk_limit / MB,
            priority=0,
            maxTaskFailures=0,
            production=False,
            tier="preemptible",
            resources=set(
                [
                    Resource(numCpus=self.cpu_limit),
                    Resource(ramMb=self.mem_limit / MB),
                    Resource(diskMb=self.disk_limit / MB),
                ]
            ),
            contactEmail="peloton-oncall-group@uber.com",
            executorConfig=self.get_executor_config(),
            container=container,
            constraints=set([host_limit]),
            requestedPorts=set(),
            mesosFetcherUris=set(),
            taskLinks={},
            metadata=set(),
        )

        job_config = JobConfiguration(
            key=self.job_key,
            owner=Identity(user=AURORA_USER),
            taskConfig=task_config,
            instanceCount=self.num_instances,
            cronSchedule=self.cron_schedule[1:],
        )
        return job_config

    def get_current_job_config(self):
        """
        Return the current job config by querying the Aurora API
        """
        resp = self.client.getJobSummary(AURORA_ROLE)
        if resp.responseCode != ResponseCode.OK:
            raise Exception(combine_messages(resp))

        job_config = None
        for s in resp.result.jobSummaryResult.summaries:
            if s.job.key == self.job_key:
                job_config = s.job
                break

        return job_config

    def update_or_create_job(self, callback):
        """
        Update the current cron-job for a Peloton app.
        Create a new cron-job if the job does not exist yet.
        """
        resp = self.client.scheduleCronJob(self.desired_job_config)
        if resp.responseCode != ResponseCode.OK:
            raise Exception(combine_messages(resp))
        return True

    def rollback_job(self):
        """
        Rollback the job config of a cron-job in case of failures
        """
        if self.current_job_config is None:
            # Nothing to do if the job doesn't exist before
            return
        resp = self.client.scheduleCronJob(self.current_job_config)
        if resp.responseCode != ResponseCode.OK:
            print("Failed to rollback watchdog configuration (%s)" % (resp))
