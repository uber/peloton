#!/usr/bin/env python

from collections import OrderedDict
import os
import requests
import random
import string
import sys
import time

import client
from docker_client import Client
import print_utils
import kind
import utils

RESOURCE_MANAGER = 1
HOST_MANAGER = 2
PLACEMENT_ENGINE = 3
JOB_MANAGER = 4
ARCHIVER = 5
AURORABRIDGE = 6
APISERVER = 7
MOCK_CQOS = 8


work_dir = os.path.dirname(os.path.abspath(__file__))


class Minicluster(object):

    def __init__(self, config, disable_mesos=False,
                 enable_k8s=False, enable_peloton=False,
                 use_host_pool=False,
                 disabled_applications={}, zk_url=None):
        self.config = config
        self.disable_mesos = disable_mesos
        self.enable_k8s = enable_k8s
        self.enable_peloton = enable_peloton
        self.disabled_applications = disabled_applications
        self.zk_url = zk_url
        self.config['use_host_pool'] = use_host_pool

        self.k8s = kind.Kind(config["k8s_cluster_name"])
        self._peloton_client = None
        self._namespace = ""  # Used for isolating miniclusters from each other
        self.cli = Client(base_url="unix://var/run/docker.sock",
                          namespace=self._namespace)
        self._create_peloton_ports()
        self._create_mesos_ports()

        global default_cluster
        default_cluster = self

        # Defines the order in which the apps are started
        # NB: HOST_MANAGER is tied to database migrations so should
        # be started first
        # TODO: Start all apps at the same time.
        self.APP_START_ORDER = OrderedDict(
            [
                (MOCK_CQOS, self.run_peloton_mockcqos),
                (HOST_MANAGER, self.run_peloton_hostmgr),
                (RESOURCE_MANAGER, self.run_peloton_resmgr),
                (PLACEMENT_ENGINE, self.run_peloton_placement),
                (JOB_MANAGER, self.run_peloton_jobmgr),
                (ARCHIVER, self.run_peloton_archiver),
                (AURORABRIDGE, self.run_peloton_aurorabridge),
                (APISERVER, self.run_peloton_apiserver),
            ]
        )

    def _create_mesos_ports(self):
        self.mesos_agent_ports = []
        base = self.config["local_agent_port"]
        for i in range(0, self.config['num_agents']):
            self.mesos_agent_ports.append(base + i)

        for i in range(0, self.config.get('num_exclusive_agents', 0)):
            self.mesos_agent_ports.append(base + self.config['num_agents'] + i)

    def _create_peloton_ports(self):
        config = self.config
        self.resmgr_ports = []
        for i in range(0, config["peloton_resmgr_instance_count"]):
            # to not cause port conflicts among apps, increase port by 10
            # for each instance
            portset = [
                port + i * 10 for port in config["peloton_resmgr_ports"]]
            self.resmgr_ports.append(portset)

        self.hostmgr_ports = []
        for i in range(0, config["peloton_hostmgr_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_hostmgr_ports"]]
            self.hostmgr_ports.append(portset)

        self.jobmgr_ports = []
        for i in range(0, config["peloton_jobmgr_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_jobmgr_ports"]]
            self.jobmgr_ports.append(portset)

        self.aurorabridge_ports = []
        for i in range(0, config["peloton_aurorabridge_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_aurorabridge_ports"]]
            self.aurorabridge_ports.append(portset)

        self.apiserver_ports = []
        for i in range(0, config["peloton_apiserver_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_apiserver_ports"]]
            self.apiserver_ports.append(portset)

        self.archiver_ports = []
        for i in range(0, config["peloton_archiver_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_archiver_ports"]]
            self.archiver_ports.append(portset)

        self.placement_ports = []
        for i in range(0, len(config["peloton_placement_instances"])):
            portset = [
                port + i * 10 for port in config["peloton_placement_ports"]]
            self.placement_ports.append(portset)

        self.mockcqos_ports = []
        for i in range(0, config["peloton_mock_cqos_instance_count"]):
            portset = [
                port + i * 10 for port in config["peloton_mock_cqos_ports"]]
            self.mockcqos_ports.append(portset)

    # Isolate changes the port numbers, container names, and other
    # config values that need to be unique on a single host.
    def isolate(self):
        # Generate a random string, which will be the "namespace" of the
        # minicluster. This namespace will be used to add random suffixes to
        # container names so that they do not collide on the same docker
        # daemon.
        # TODO: Use this namespace.
        letters = string.ascii_lowercase
        rand_str = ''.join(random.choice(letters) for i in range(3))
        self._namespace = rand_str + '-'
        self.cli = Client(base_url="unix://var/run/docker.sock",
                          namespace=self._namespace)

        # Now we need to randomize the ports.
        # TODO: Fix race condition between the find_free_port() and the process
        # that will actually bind to that port.
        self.config["local_master_port"] = utils.find_free_port()
        self.config["local_zk_port"] = utils.find_free_port()
        self.config["local_cassandra_cql_port"] = utils.find_free_port()
        self.config["local_cassandra_thrift_port"] = utils.find_free_port()

        self.mesos_agent_ports = utils.randomize_ports(self.mesos_agent_ports)

        self.resmgr_ports = utils.randomize_ports(self.resmgr_ports)
        self.hostmgr_ports = utils.randomize_ports(self.hostmgr_ports)
        self.jobmgr_ports = utils.randomize_ports(self.jobmgr_ports)
        self.aurorabridge_ports = utils.randomize_ports(
            self.aurorabridge_ports)
        self.apiserver_ports = utils.randomize_ports(self.apiserver_ports)
        self.archiver_ports = utils.randomize_ports(self.archiver_ports)
        self.placement_ports = utils.randomize_ports(self.placement_ports)
        self.mockcqos_ports = utils.randomize_ports(self.mockcqos_ports)

        # TODO: Save those to local disk, or print them to stdout.
        return self._namespace

    def setup(self):
        if self.enable_k8s:
            self._setup_k8s()

        if not self.disable_mesos:
            self._setup_mesos()

        self._setup_cassandra()
        if self.enable_peloton:
            self._setup_peloton()

    def teardown(self, stop=False):
        print_utils.okgreen("teardown started...")
        self._teardown_peloton(stop)
        self._teardown_mesos()
        self._teardown_k8s()
        self._teardown_cassandra()
        self._teardown_zk()
        print_utils.okgreen("teardown complete!")

    def set_mesos_agent_exclusive(self, index, exclusive_label_value):
        self.teardown_mesos_agent(index, is_exclusive=False)
        port = self.mesos_agent_ports[index]
        self.setup_mesos_agent(
            index, port, is_exclusive=True,
            exclusive_label_value=exclusive_label_value)

    def set_mesos_agent_nonexclusive(self, index):
        self.teardown_mesos_agent(index, is_exclusive=True)
        port = self.mesos_agent_ports[index]
        self.setup_mesos_agent(index, port)

    def setup_mesos_agent(self, index, local_port, is_exclusive=False,
                          exclusive_label_value=''):
        config = self.config
        prefix = config["mesos_agent_container"]
        attributes = config["attributes"]
        if is_exclusive:
            prefix += "-exclusive"
            attributes += ";peloton/exclusive:" + exclusive_label_value
        agent = prefix + repr(index)
        ctn_port = config["agent_port"]
        container = self.cli.create_container(
            name=agent,
            hostname=agent,
            volumes=["/files", "/var/run/docker.sock"],
            ports=[repr(ctn_port)],
            host_config=self.cli.create_host_config(
                port_bindings={ctn_port: local_port},
                binds=[
                    work_dir + "/files:/files",
                    work_dir
                    + "/mesos_config/etc_mesos-slave:/etc/mesos-slave",
                    "/var/run/docker.sock:/var/run/docker.sock",
                ],
                privileged=True,
            ),
            environment=[
                "MESOS_PORT=" + repr(ctn_port),
                "MESOS_MASTER=zk://{0}:{1}/mesos".format(
                    self.cli.get_container_ip(config["zk_container"]),
                    config["default_zk_port"],
                ),
                "MESOS_SWITCH_USER=" + repr(config["switch_user"]),
                "MESOS_CONTAINERIZERS=" + config["containers"],
                "MESOS_LOG_DIR=" + config["log_dir"],
                "MESOS_ISOLATION=" + config["isolation"],
                "MESOS_SYSTEMD_ENABLE_SUPPORT=false",
                "MESOS_IMAGE_PROVIDERS=" + config["image_providers"],
                "MESOS_IMAGE_PROVISIONER_BACKEND={0}".format(
                    config["image_provisioner_backend"]
                ),
                "MESOS_APPC_STORE_DIR=" + config["appc_store_dir"],
                "MESOS_WORK_DIR=" + config["work_dir"],
                "MESOS_RESOURCES=" + config["resources"],
                "MESOS_ATTRIBUTES=" + attributes,
                "MESOS_MODULES=" + config["modules"],
                "MESOS_RESOURCE_ESTIMATOR=" + config["resource_estimator"],
                "MESOS_OVERSUBSCRIBED_RESOURCES_INTERVAL="
                + config["oversubscribed_resources_interval"],
                "MESOS_QOS_CONTROLLER=" + config["qos_controller"],
                "MESOS_QOS_CORRECTION_INTERVAL_MIN="
                + config["qos_correction_interval_min"],
            ],
            image=config["mesos_slave_image"],
            entrypoint="bash /files/run_mesos_slave.sh",
            detach=True,
        )
        self.cli.start(container=container.get("Id"))
        utils.wait_for_up(agent, local_port, "state.json")

    def teardown_mesos_agent(self, index, is_exclusive=False):
        prefix = self.config["mesos_agent_container"]
        if is_exclusive:
            prefix += "-exclusive"
        agent = prefix + repr(index)
        self.cli.remove_existing_container(agent)

    def _setup_k8s(self):
        print_utils.okgreen("starting k8s cluster")
        self.k8s.teardown()
        self.k8s.create()
        print_utils.okgreen("started k8s cluster")

    def _teardown_k8s(self):
        self.k8s.teardown()

    def _setup_mesos(self):
        self._teardown_mesos()
        self._teardown_zk()
        self._setup_zk()
        self._setup_mesos_master()
        self._setup_mesos_agents()

    def _teardown_mesos(self):
        # 1 - Remove all Mesos Agents
        for i in range(0, self.config["num_agents"]):
            self.teardown_mesos_agent(i)

        for i in range(0, self.config.get("num_exclusive_agents", 0)):
            self.teardown_mesos_agent(i, is_exclusive=True)

        # 2 - Remove Mesos Master
        self.cli.remove_existing_container(
            self.config["mesos_master_container"])

        # 3- Remove orphaned mesos containers.
        for c in self.cli.containers(filters={"name": "^/mesos-"}, all=True):
            self.cli.remove_existing_container(c.get("Id"))

    def _setup_zk(self):
        config = self.config
        self.cli.pull(config["zk_image"])
        container = self.cli.create_container(
            name=config["zk_container"],
            hostname=config["zk_container"],
            host_config=self.cli.create_host_config(
                port_bindings={
                    config["default_zk_port"]: config["local_zk_port"],
                },
            ),
            image=config["zk_image"],
            detach=True,
        )
        self.cli.start(container=container.get("Id"))
        print_utils.okgreen("started container %s" % config["zk_container"])
        print_utils.okgreen("waiting on %s to be rdy" % config["zk_container"])

        count = 0
        while count < utils.max_retry_attempts:
            count += 1
            if utils.is_zk_ready(config["local_zk_port"]):
                return
            time.sleep(utils.sleep_time_secs)

        raise Exception("zk failed to come up in time")

    def _teardown_zk(self):
        self.cli.remove_existing_container(self.config["zk_container"])

    def _setup_cassandra(self):
        config = self.config
        self.cli.remove_existing_container(config["cassandra_container"])
        self.cli.pull(config["cassandra_image"])
        container = self.cli.create_container(
            name=config["cassandra_container"],
            hostname=config["cassandra_container"],
            host_config=self.cli.create_host_config(
                port_bindings={
                    config["cassandra_cql_port"]: config[
                        "local_cassandra_cql_port"
                    ],
                    config["cassandra_thrift_port"]: config[
                        "local_cassandra_thrift_port"
                    ],
                },
                binds=[work_dir + "/files:/files"],
            ),
            environment=["MAX_HEAP_SIZE=1G", "HEAP_NEWSIZE=256M"],
            image=config["cassandra_image"],
            detach=True,
            entrypoint="bash /files/run_cassandra_with_stratio_index.sh",
        )
        self.cli.start(container=container.get("Id"))
        print_utils.okgreen("started container %s" %
                            config["cassandra_container"])

        self._create_cassandra_store()

    def _teardown_cassandra(self):
        self.cli.remove_existing_container(self.config["cassandra_container"])

    def _setup_mesos_master(self):
        config = self.config
        self.cli.pull(config["mesos_master_image"])
        container = self.cli.create_container(
            name=config["mesos_master_container"],
            hostname=config["mesos_master_container"],
            volumes=["/files"],
            ports=[repr(config["master_port"])],
            host_config=self.cli.create_host_config(
                port_bindings={config["master_port"]: config[
                    "local_master_port"
                ]},
                binds=[
                    work_dir + "/files:/files",
                    work_dir + "/mesos_config/etc_mesos-master:/etc/mesos-master",
                ],
                privileged=True,
            ),
            environment=[
                "MESOS_AUTHENTICATE_HTTP_READWRITE=true",
                "MESOS_AUTHENTICATE_FRAMEWORKS=true",
                # TODO: Enable following flags for fully authentication.
                "MESOS_AUTHENTICATE_HTTP_FRAMEWORKS=true",
                "MESOS_HTTP_FRAMEWORK_AUTHENTICATORS=basic",
                "MESOS_CREDENTIALS=/etc/mesos-master/credentials",
                "MESOS_LOG_DIR=" + config["log_dir"],
                "MESOS_PORT=" + repr(config["master_port"]),
                "MESOS_ZK=zk://{0}:{1}/mesos".format(
                    self.cli.get_container_ip(config["zk_container"]),
                    config["default_zk_port"],
                ),
                "MESOS_QUORUM=" + repr(config["quorum"]),
                "MESOS_REGISTRY=" + config["registry"],
                "MESOS_WORK_DIR=" + config["work_dir"],
            ],
            image=config["mesos_master_image"],
            entrypoint="bash /files/run_mesos_master.sh",
            detach=True,
        )
        self.cli.start(container=container.get("Id"))
        master_container = config["mesos_master_container"]
        print_utils.okgreen("started container %s" % master_container)

    def _setup_mesos_agents(self):
        config = self.config
        self.cli.pull(config['mesos_slave_image'])
        for i in range(0, config['num_agents']):
            port = self.mesos_agent_ports[i]
            self.setup_mesos_agent(i, port)

        for i in range(0, config.get('num_exclusive_agents', 0)):
            port = self.mesos_agent_ports[config['num_agents'] + i]
            self.setup_mesos_agent(
                i, port,
                is_exclusive=True,
                exclusive_label_value=config.get('exclusive_label_value', ''))

    def _create_cassandra_store(self):
        config = self.config
        retry_attempts = 0
        while retry_attempts < utils.max_retry_attempts:
            time.sleep(utils.sleep_time_secs)
            setup_exe = self.cli.exec_create(
                container=config["cassandra_container"],
                cmd="/files/setup_cassandra.sh",
            )
            show_exe = self.cli.exec_create(
                container=config["cassandra_container"],
                cmd='cqlsh -e "describe %s"' % config["cassandra_test_db"],
            )
            # by api design, exec_start needs to be called after exec_create
            # to run 'docker exec'
            resp = self.cli.exec_start(exec_id=setup_exe)
            if resp == "":
                resp = self.cli.exec_start(exec_id=show_exe)
                if "CREATE KEYSPACE peloton_test WITH" in resp:
                    print_utils.okgreen("cassandra store is created")
                    return
            if retry_attempts % 5 == 1:
                print_utils.warn("failed to create c* store, retrying...")
            retry_attempts += 1

        print_utils.fail(
            "Failed to create cassandra store after %d attempts, "
            "aborting..." % utils.max_retry_attempts
        )
        sys.exit(1)

    def _setup_peloton(self):
        print_utils.okblue(
            'docker image "uber/peloton" has to be built first '
            "locally by running IMAGE=uber/peloton make docker"
        )

        for app, func in self.APP_START_ORDER.iteritems():
            if app in self.disabled_applications:
                should_disable = self.disabled_applications[app]
                if should_disable:
                    continue
            self.APP_START_ORDER[app]()

    def _teardown_peloton(self, stop):
        config = self.config
        if stop:
            # Stop existing container
            func = self.cli.stop_container
        else:
            # Remove existing container
            func = self.cli.remove_existing_container

        # 1 - Remove jobmgr instances
        for i in range(0, config["peloton_jobmgr_instance_count"]):
            name = config["peloton_jobmgr_container"] + repr(i)
            func(name)

        # 2 - Remove placement engine instances
        for i in range(0, len(config["peloton_placement_instances"])):
            name = config["peloton_placement_container"] + repr(i)
            func(name)

        # 3 - Remove resmgr instances
        for i in range(0, config["peloton_resmgr_instance_count"]):
            name = config["peloton_resmgr_container"] + repr(i)
            func(name)

        # 4 - Remove hostmgr instances
        for i in range(0, config["peloton_hostmgr_instance_count"]):
            name = config["peloton_hostmgr_container"] + repr(i)
            func(name)

        # 5 - Remove archiver instances
        for i in range(0, config["peloton_archiver_instance_count"]):
            name = config["peloton_archiver_container"] + repr(i)
            func(name)

        # 6 - Remove aurorabridge instances
        for i in range(0, config["peloton_aurorabridge_instance_count"]):
            name = config["peloton_aurorabridge_container"] + repr(i)
            func(name)

        # 7 - Remove apiserver instances
        for i in range(0, config["peloton_apiserver_instance_count"]):
            name = config["peloton_apiserver_container"] + repr(i)
            func(name)

        # 8 - Remove mock-cqos instances
        for i in range(0, config["peloton_mock_cqos_instance_count"]):
            name = config["peloton_mock_cqos_container"] + repr(i)
            func(name)

    # Run peloton resmgr app

    def run_peloton_resmgr(self):
        env = {}
        if self.enable_k8s:
            env.update({"HOSTMGR_API_VERSION": "v1alpha"})

        # TODO: move docker run logic into a common function for all
        # apps to share
        config = self.config
        for i in range(0, config["peloton_resmgr_instance_count"]):
            ports = self.resmgr_ports[i]
            name = config["peloton_resmgr_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait(
                "resmgr",
                name,
                ports,
                extra_env=env,
            )

    # Run peloton hostmgr app
    def run_peloton_hostmgr(self):
        config = self.config
        scarce_resource = ",".join(config["scarce_resource_types"])
        slack_resource = ",".join(config["slack_resource_types"])
        mounts = []
        env = {
            "SCARCE_RESOURCE_TYPES": scarce_resource,
            "SLACK_RESOURCE_TYPES": slack_resource,
            "ENABLE_HOST_POOL": True,
        }
        if self.enable_k8s:
            kubeconfig_dir = os.path.dirname(self.k8s.get_kubeconfig())
            mounts = [kubeconfig_dir + ":/.kube"]
            # Always enable host pool in hostmgr of mini cluster.
            env.update({
                "ENABLE_K8S": True,
                "KUBECONFIG": "/.kube/kind-config-peloton-k8s",
            })

        for i in range(0, config["peloton_hostmgr_instance_count"]):
            ports = self.hostmgr_ports[i]
            name = config["peloton_hostmgr_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait(
                "hostmgr",
                name,
                ports,
                extra_env=env,
                mounts=mounts,
            )

    # Run peloton jobmgr app
    def run_peloton_jobmgr(self):
        config = self.config
        env = {
            "MESOS_AGENT_WORK_DIR": config["work_dir"],
            "JOB_TYPE": os.getenv("JOB_TYPE", "BATCH"),
        }
        if self.enable_k8s:
            env.update({"HOSTMGR_API_VERSION": "v1alpha"})

        for i in range(0, config["peloton_jobmgr_instance_count"]):
            ports = self.jobmgr_ports[i]
            name = config["peloton_jobmgr_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait(
                "jobmgr",
                name,
                ports,
                extra_env=env,
            )

    # Run peloton aurora bridge app
    def run_peloton_aurorabridge(self):
        config = self.config
        for i in range(0, config["peloton_aurorabridge_instance_count"]):
            ports = self.aurorabridge_ports[i]
            name = config["peloton_aurorabridge_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait("aurorabridge", name, ports)

    # Run peloton placement app
    def run_peloton_placement(self):
        i = 0
        config = self.config
        for task_type in config["peloton_placement_instances"]:
            ports = self.placement_ports[i]
            name = config["peloton_placement_container"] + repr(i)
            self.cli.remove_existing_container(name)
            if task_type == 'BATCH':
                app_type = 'placement'
            else:
                app_type = 'placement_' + task_type.lower()
            env = {
                "APP_TYPE": app_type,
                "TASK_TYPE": task_type,
                "USE_HOST_POOL": config.get("use_host_pool", False),
            }
            if self.enable_k8s:
                env.update({"HOSTMGR_API_VERSION": "v1alpha"})
            self.start_and_wait(
                "placement",
                name,
                ports,
                extra_env=env,
            )
            i = i + 1

    # Run peloton api server
    def run_peloton_apiserver(self):
        config = self.config
        for i in range(0, config["peloton_apiserver_instance_count"]):
            ports = self.apiserver_ports[i]
            name = config["peloton_apiserver_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait("apiserver", name, ports)

    # Run peloton mock-cqos server
    def run_peloton_mockcqos(self):
        config = self.config
        for i in range(0, config["peloton_mock_cqos_instance_count"]):
            ports = self.mockcqos_ports[i]
            name = config["peloton_mock_cqos_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait("mock-cqos", name, ports)

    # Run peloton archiver app
    def run_peloton_archiver(self):
        config = self.config
        for i in range(0, config["peloton_archiver_instance_count"]):
            ports = self.archiver_ports[i]
            name = config["peloton_archiver_container"] + repr(i)
            self.cli.remove_existing_container(name)
            self.start_and_wait(
                "archiver",
                name,
                ports,
            )

    # Starts a container and waits for it to come up
    def start_and_wait(self, application_name, container_name, ports,
                       extra_env=None, mounts=None):
        if mounts is None:
            mounts = []

        # TODO: It's very implicit that the first port is the HTTP
        # port, perhaps we should split it out even more.
        election_zk_servers = None
        mesos_zk_path = None
        zk_url = self.zk_url
        config = self.config
        if zk_url is not None:
            election_zk_servers = zk_url
            mesos_zk_path = "zk://{0}/mesos".format(zk_url)
        else:
            election_zk_servers = "{0}:{1}".format(
                self.cli.get_container_ip(config["zk_container"]),
                config["default_zk_port"],
            )
            mesos_zk_path = "zk://{0}:{1}/mesos".format(
                self.cli.get_container_ip(config["zk_container"]),
                config["default_zk_port"],
            )
        cass_hosts = self.cli.get_container_ip(config["cassandra_container"])
        env = {
            "CONFIG_DIR": "config",
            "APP": application_name,
            "HTTP_PORT": ports[0],
            "DB_HOST": cass_hosts,
            "ELECTION_ZK_SERVERS": election_zk_servers,
            "MESOS_ZK_PATH": mesos_zk_path,
            "MESOS_SECRET_FILE": "/files/hostmgr_mesos_secret",
            "CASSANDRA_HOSTS": cass_hosts,
            "ENABLE_DEBUG_LOGGING": config["debug"],
            "DATACENTER": "",
            # used to migrate the schema;used inside host manager
            "AUTO_MIGRATE": config["auto_migrate"],
            "CLUSTER": "minicluster",
            'AUTH_TYPE': os.getenv('AUTH_TYPE', 'NOOP'),
            'AUTH_CONFIG_FILE': os.getenv('AUTH_CONFIG_FILE'),
        }
        if len(ports) > 1:
            env["GRPC_PORT"] = ports[1]
        if extra_env:
            env.update(extra_env)
        environment = []
        for key, value in env.iteritems():
            environment.append("%s=%s" % (key, value))
        # BIND_MOUNTS allows additional files to be mounted in the
        # the container. Expected format is a comma-separated list
        # of items of the form <host-path>:<container-path>
        extra_mounts = os.environ.get("BIND_MOUNTS", "").split(",") or []
        mounts.extend(list(filter(None, extra_mounts)))
        container = self.cli.create_container(
            name=container_name,
            hostname=container_name,
            ports=[repr(port) for port in ports],
            environment=environment,
            host_config=self.cli.create_host_config(
                port_bindings={port: port for port in ports},
                binds=[work_dir + "/files:/files"] + mounts,
            ),
            # pull or build peloton image if not exists
            image=config["peloton_image"],
            detach=True,
        )
        self.cli.start(container=container.get("Id"))
        utils.wait_for_up(
            container_name, ports[0]
        )  # use the first port as primary

    def peloton_client(self):
        if self._peloton_client is not None:
            return self._peloton_client

        name = self.config.get("name", "standard-minicluster")
        zk_servers = "localhost:{}".format(self.config["local_zk_port"])
        use_apiserver = os.getenv("USE_APISERVER") == 'True'
        grpc = "grpc://localhost:{}"
        self._peloton_client = client.PelotonClientWrapper(
            name=name,
            zk_servers=zk_servers,
            enable_apiserver=use_apiserver,
            api_url=grpc.format(self.apiserver_ports[0][1]),
            jm_url=grpc.format(self.jobmgr_ports[0][1]),
            rm_url=grpc.format(self.resmgr_ports[0][1]),
            hm_url=grpc.format(self.hostmgr_ports[0][1]),
        )
        return self._peloton_client

    def wait_for_mesos_master_leader(self, timeout_secs=20):
        """
        util method to wait for mesos master leader elected
        """

        port = self.config.get("local_master_port")
        url = "{}:{}/state.json".format(utils.HTTP_LOCALHOST, port)
        print_utils.warn("waiting for mesos master leader")
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            try:
                resp = requests.get(url)
                if resp.status_code != 200:
                    time.sleep(1)
                    continue
                print_utils.okgreen("mesos master is ready")
                return
            except Exception:
                pass

        assert False, "timed out waiting for mesos master leader"

    def wait_for_all_agents_to_register(self, agent_count=3, timeout_secs=300):
        """
        util method to wait for all agents to register
        """
        port = self.config.get("local_master_port")
        url = "{}:{}/state.json".format(utils.HTTP_LOCALHOST, port)
        print_utils.warn("waiting for all mesos agents")

        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            try:
                resp = requests.get(url)
                if resp.status_code == 200:
                    registered_agents = 0
                    for a in resp.json()['slaves']:
                        if a['active']:
                            registered_agents += 1

                    if registered_agents == agent_count:
                        print_utils.okgreen("all mesos agents are ready")
                        return
                time.sleep(1)
            except Exception:
                pass


default_cluster = Minicluster(utils.default_config())
