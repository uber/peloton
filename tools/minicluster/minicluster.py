#!/usr/bin/env python

import os
import sys
import time
from docker import Client

import print_utils
import kind
import utils

PELOTON_K8S_NAME = "peloton-k8s"

zk_url = None
cli = Client(base_url="unix://var/run/docker.sock")
work_dir = os.path.dirname(os.path.abspath(__file__))


# Delete the kind cluster.
def teardown_k8s():
    k8s = kind.Kind(PELOTON_K8S_NAME)
    try:
        return k8s.teardown()
    except OSError as e:
        if e.errno == 2:
            print_utils.warn("kubernetes was not running")
            return True
        else:
            raise


def teardown_mesos_agent(config, agent_index, is_exclusive=False):
    prefix = config["mesos_agent_container"]
    if is_exclusive:
        prefix += "-exclusive"
    agent = prefix + repr(agent_index)
    utils.remove_existing_container(agent)


#
# Teardown mesos related containers.
#
def teardown_mesos(config):
    # 1 - Remove all Mesos Agents
    for i in range(0, config["num_agents"]):
        teardown_mesos_agent(config, i)
    for i in range(0, config.get("num_exclusive_agents", 0)):
        teardown_mesos_agent(config, i, is_exclusive=True)

    # 2 - Remove Mesos Master
    utils.remove_existing_container(config["mesos_master_container"])

    # 3- Remove orphaned mesos containers.
    for c in cli.containers(filters={"name": "^/mesos-"}, all=True):
        utils.remove_existing_container(c.get("Id"))

    # 4 - Remove ZooKeeper
    utils.remove_existing_container(config["zk_container"])


# Start a kind cluster.
def run_k8s():
    k8s = kind.Kind(PELOTON_K8S_NAME)
    k8s.teardown()
    k8s.create()


#
# Run mesos cluster
#
def run_mesos(config):
    # Remove existing containers first.
    teardown_mesos(config)

    # Run zk
    cli.pull(config["zk_image"])
    container = cli.create_container(
        name=config["zk_container"],
        hostname=config["zk_container"],
        host_config=cli.create_host_config(
            port_bindings={config["default_zk_port"]: config["local_zk_port"]}
        ),
        image=config["zk_image"],
        detach=True,
    )
    cli.start(container=container.get("Id"))
    print_utils.okgreen("started container %s" % config["zk_container"])

    # TODO: add retry
    print_utils.okblue("sleep 20 secs for zk to come up")
    time.sleep(20)

    # Run mesos master
    cli.pull(config["mesos_master_image"])
    container = cli.create_container(
        name=config["mesos_master_container"],
        hostname=config["mesos_master_container"],
        volumes=["/files"],
        ports=[repr(config["master_port"])],
        host_config=cli.create_host_config(
            port_bindings={config["master_port"]: config["master_port"]},
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
                utils.get_container_ip(config["zk_container"]),
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
    cli.start(container=container.get("Id"))
    master_container = config["mesos_master_container"]
    print_utils.okgreen("started container %s" % master_container)

    # Run mesos slaves
    cli.pull(config["mesos_slave_image"])
    for i in range(0, config["num_agents"]):
        run_mesos_agent(config, i, i)
    for i in range(0, config.get("num_exclusive_agents", 0)):
        run_mesos_agent(
            config,
            i,
            config["num_agents"] + i,
            is_exclusive=True,
            exclusive_label_value=config.get("exclusive_label_value", ""),
        )


#
# Run a mesos agent
#
def run_mesos_agent(
        config,
        agent_index,
        port_offset,
        is_exclusive=False,
        exclusive_label_value="",
):
    prefix = config["mesos_agent_container"]
    attributes = config["attributes"]
    if is_exclusive:
        prefix += "-exclusive"
        attributes += ";peloton/exclusive:" + exclusive_label_value
    agent = prefix + repr(agent_index)
    port = config["local_agent_port"] + port_offset
    container = cli.create_container(
        name=agent,
        hostname=agent,
        volumes=["/files", "/var/run/docker.sock"],
        ports=[repr(config["default_agent_port"])],
        host_config=cli.create_host_config(
            port_bindings={config["default_agent_port"]: port},
            binds=[
                work_dir + "/files:/files",
                work_dir + "/mesos_config/etc_mesos-slave:/etc/mesos-slave",
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
            privileged=True,
        ),
        environment=[
            "MESOS_PORT=" + repr(port),
            "MESOS_MASTER=zk://{0}:{1}/mesos".format(
                utils.get_container_ip(config["zk_container"]),
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
    cli.start(container=container.get("Id"))
    print_utils.okgreen("started container %s" % agent)


#
# Run cassandra cluster
#
def run_cassandra(config):
    utils.remove_existing_container(config["cassandra_container"])
    cli.pull(config["cassandra_image"])
    container = cli.create_container(
        name=config["cassandra_container"],
        hostname=config["cassandra_container"],
        host_config=cli.create_host_config(
            port_bindings={
                config["cassandra_cql_port"]: config["cassandra_cql_port"],
                config["cassandra_thrift_port"]: config[
                    "cassandra_thrift_port"
                ],
            },
            binds=[work_dir + "/files:/files"],
        ),
        environment=["MAX_HEAP_SIZE=1G", "HEAP_NEWSIZE=256M"],
        image=config["cassandra_image"],
        detach=True,
        entrypoint="bash /files/run_cassandra_with_stratio_index.sh",
    )
    cli.start(container=container.get("Id"))
    print_utils.okgreen("started container %s" % config["cassandra_container"])

    # Create cassandra store
    create_cassandra_store(config)


#
# Create cassandra store with retries
#
def create_cassandra_store(config):
    retry_attempts = 0
    while retry_attempts < utils.max_retry_attempts:
        time.sleep(utils.sleep_time_secs)
        setup_exe = cli.exec_create(
            container=config["cassandra_container"],
            cmd="/files/setup_cassandra.sh",
        )
        show_exe = cli.exec_create(
            container=config["cassandra_container"],
            cmd='cqlsh -e "describe %s"' % config["cassandra_test_db"],
        )
        # by api design, exec_start needs to be called after exec_create
        # to run 'docker exec'
        resp = cli.exec_start(exec_id=setup_exe)
        if resp == "":
            resp = cli.exec_start(exec_id=show_exe)
            if "CREATE KEYSPACE peloton_test WITH" in resp:
                print_utils.okgreen("cassandra store is created")
                return
        print_utils.warn("failed to create cassandra store, retrying...")
        retry_attempts += 1

    print_utils.fail(
        "Failed to create cassandra store after %d attempts, "
        "aborting..." % utils.max_retry_attempts
    )
    sys.exit(1)


#
# Starts a container and waits for it to come up
#
def start_and_wait(
        application_name, container_name, ports, config, extra_env=None
):
    # TODO: It's very implicit that the first port is the HTTP port, perhaps we
    # should split it out even more.
    election_zk_servers = None
    mesos_zk_path = None
    if zk_url is not None:
        election_zk_servers = zk_url
        mesos_zk_path = "zk://{0}/mesos".format(zk_url)
    else:
        election_zk_servers = "{0}:{1}".format(
            utils.get_container_ip(config["zk_container"]),
            config["default_zk_port"],
        )
        mesos_zk_path = "zk://{0}:{1}/mesos".format(
            utils.get_container_ip(config["zk_container"]),
            config["default_zk_port"],
        )
    cass_hosts = utils.get_container_ip(config["cassandra_container"])
    env = {
        "CONFIG_DIR": "config",
        "APP": application_name,
        "HTTP_PORT": ports[0],
        "DB_HOST": utils.get_container_ip(config["cassandra_container"]),
        "ELECTION_ZK_SERVERS": election_zk_servers,
        "MESOS_ZK_PATH": mesos_zk_path,
        "MESOS_SECRET_FILE": "/files/hostmgr_mesos_secret",
        "CASSANDRA_HOSTS": cass_hosts,
        "ENABLE_DEBUG_LOGGING": config["debug"],
        "DATACENTER": "",
        # used to migrate the schema;used inside host manager
        "AUTO_MIGRATE": config["auto_migrate"],
        "CLUSTER": "minicluster",
        "AUTH_TYPE": os.getenv("AUTH_TYPE", "NOOP"),
        "AUTH_CONFIG_FILE": os.getenv("AUTH_CONFIG_FILE"),
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
    mounts = os.environ.get("BIND_MOUNTS", "")
    mounts = mounts.split(",") if mounts else []
    container = cli.create_container(
        name=container_name,
        hostname=container_name,
        ports=[repr(port) for port in ports],
        environment=environment,
        host_config=cli.create_host_config(
            port_bindings={port: port for port in ports},
            binds=[work_dir + "/files:/files"] + mounts,
        ),
        # pull or build peloton image if not exists
        image=config["peloton_image"],
        detach=True,
    )
    cli.start(container=container.get("Id"))
    utils.wait_for_up(
        container_name, ports[0]
    )  # use the first port as primary


#
# Run peloton resmgr app
#
def run_peloton_resmgr(config):
    # TODO: move docker run logic into a common function for all apps to share
    for i in range(0, config["peloton_resmgr_instance_count"]):
        # to not cause port conflicts among apps, increase port by 10
        # for each instance
        ports = [port + i * 10 for port in config["peloton_resmgr_ports"]]
        name = config["peloton_resmgr_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait("resmgr", name, ports, config)


#
# Run peloton hostmgr app
#
def run_peloton_hostmgr(config):
    for i in range(0, config["peloton_hostmgr_instance_count"]):
        # to not cause port conflicts among apps, increase port
        # by 10 for each instance
        ports = [port + i * 10 for port in config["peloton_hostmgr_ports"]]
        scarce_resource = ",".join(config["scarce_resource_types"])
        slack_resource = ",".join(config["slack_resource_types"])
        name = config["peloton_hostmgr_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait(
            "hostmgr",
            name,
            ports,
            config,
            extra_env={
                "SCARCE_RESOURCE_TYPES": scarce_resource,
                "SLACK_RESOURCE_TYPES": slack_resource,
            },
        )


#
# Run peloton jobmgr app
#
def run_peloton_jobmgr(config):
    for i in range(0, config["peloton_jobmgr_instance_count"]):
        # to not cause port conflicts among apps, increase port by 10
        #  for each instance
        ports = [port + i * 10 for port in config["peloton_jobmgr_ports"]]
        name = config["peloton_jobmgr_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait(
            "jobmgr",
            name,
            ports,
            config,
            extra_env={
                "MESOS_AGENT_WORK_DIR": config["work_dir"],
                "JOB_TYPE": os.getenv("JOB_TYPE", "BATCH"),
            },
        )


#
# Run peloton aurora bridge app
#
def run_peloton_aurorabridge(config):
    for i in range(0, config["peloton_aurorabridge_instance_count"]):
        ports = [
            port + i * 10 for port in config["peloton_aurorabridge_ports"]
        ]
        name = config["peloton_aurorabridge_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait("aurorabridge", name, ports, config)


#
# Run peloton placement app
#
def run_peloton_placement(config):
    i = 0
    for task_type in config["peloton_placement_instances"]:
        # to not cause port conflicts among apps, increase port by 10
        # for each instance
        ports = [port + i * 10 for port in config["peloton_placement_ports"]]
        name = config["peloton_placement_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait(
            "placement",
            name,
            ports,
            config,
            extra_env={
                "TASK_TYPE": task_type,
            },
        )
        i = i + 1


#
# Run peloton archiver app
#
def run_peloton_archiver(config):
    for i in range(0, config["peloton_archiver_instance_count"]):
        ports = [port + i * 10 for port in config["peloton_archiver_ports"]]
        name = config["peloton_archiver_container"] + repr(i)
        utils.remove_existing_container(name)
        start_and_wait("archiver", name, ports, config)
