#!/usr/bin/env python
"""
 -- Locally run and manage a personal cluster in containers.

This script can be used to manage (setup, teardown) a personal
Mesos cluster and Mysql etc in containers, optionally Peloton
master or apps can be specified to run in containers as well.

@copyright:  2017 Uber Compute Platform. All rights reserved.

@license:    license

@contact:    peloton-dev@uber.com
"""

import os
import requests
import sys
import time
import yaml
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from collections import OrderedDict
from docker import Client

__date__ = '2016-12-08'
__author__ = 'wu'

max_retry_attempts = 20
sleep_time_secs = 5
healthcheck_path = '/health'
default_host = '127.0.0.1'


class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'


def print_okblue(message):
    print bcolors.OKBLUE + message + bcolors.ENDC


def print_okgreen(message):
    print bcolors.OKGREEN + message + bcolors.ENDC


def print_fail(message):
    print bcolors.FAIL + message + bcolors.ENDC


def print_warn(message):
    print bcolors.WARNING + message + bcolors.ENDC


#
# Get eth0 ip of the host
# TODO: see if we can do better than running ipconfig/ifconfig
#
def get_host_ip():
    uname = os.uname()[0]
    if uname == "Darwin":
        ip = os.popen('ipconfig getifaddr en0').read()
    else:
        # The command hostname -I gives a list of ip's for all network
        # interfaces on the host and we just pick the first ip from that
        # list. On a Debian Jessi box this returns:
        #   10.162.17.29 10.162.81.29 172.17.0.1
        # On a Ubuntu Trusty Tahr box this returns:
        #   172.24.98.94 172.17.0.1
        ip = os.popen('hostname -I').read().strip().split(' ')[0]
    return ip.strip()


#
# Load configs from file
#
def load_config():
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "config.yaml")
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


host_ip = get_host_ip()
cli = Client(base_url='unix://var/run/docker.sock')
work_dir = os.path.dirname(os.path.abspath(__file__))
config = load_config()


#
# Force remove container by name (best effort)
#
def remove_existing_container(name):
    try:
        cli.remove_container(name, force=True)
        print_okblue('removed container %s' % name)
    except Exception, e:
        if 'No such container' in str(e):
            return
        raise e


#
# Teardown mesos related containers.
#
def teardown_mesos():
    remove_existing_container(config['zk_container'])

    remove_existing_container(config['mesos_master_container'])

    for i in range(0, config['num_agents']):
        agent = config['mesos_agent_container'] + repr(i)
        remove_existing_container(agent)

    # Remove orphaned mesos containers.
    for c in cli.containers(filters={'name': '^/mesos-'}, all=True):
        remove_existing_container(c.get("Id"))


#
# Run mesos cluster
#
def run_mesos():
    # Remove existing containers first.
    teardown_mesos()

    # Run zk
    cli.pull(config['zk_image'])
    container = cli.create_container(
        name=config['zk_container'],
        hostname=config['zk_container'],
        volumes=['/files'],
        host_config=cli.create_host_config(
            port_bindings={
                config['default_zk_port']: config['local_zk_port'],
            },
            binds=[
                work_dir + '/files:/files',
            ]),
        image=config['zk_image'],
        detach=True
    )
    cli.start(container=container.get('Id'))
    print_okgreen('started container %s' % config['zk_container'])

    # TODO: add retry
    print_okblue('sleep 20 secs for zk to come up')
    time.sleep(20)

    # Run mesos master
    cli.pull(config['mesos_master_image'])
    container = cli.create_container(
        name=config['mesos_master_container'],
        hostname=config['mesos_master_container'],
        volumes=['/files'],
        ports=[repr(config['master_port'])],
        host_config=cli.create_host_config(
            port_bindings={
                config['master_port']: config['master_port'],
            },
            binds=[
                work_dir + '/files:/files',
                work_dir + '/mesos_config/etc_mesos-master:/etc/mesos-master'
            ],
            privileged=True
        ),
        environment=[
            'MESOS_LOG_DIR=' + config['log_dir'],
            'MESOS_PORT=' + repr(config['master_port']),
            'MESOS_ZK=zk://{0}:{1}/mesos'.format(
                host_ip,
                config['local_zk_port']),
            'MESOS_QUORUM=' + repr(config['quorum']),
            'MESOS_REGISTRY=' + config['registry'],
            'MESOS_WORK_DIR=' + config['work_dir'],
            'MESOS_ADVERTISE_IP={}'.format(host_ip),
        ],
        image=config['mesos_master_image'],
        entrypoint='bash /files/run_mesos_master.sh',
        detach=True,
    )
    cli.start(container=container.get('Id'))
    print_okgreen('started container %s' % config['mesos_master_container'])

    # Run mesos slaves
    cli.pull(config['mesos_slave_image'])
    for i in range(0, config['num_agents']):
        agent = config['mesos_agent_container'] + repr(i)
        port = config['local_agent_port'] + i
        container = cli.create_container(
            name=agent,
            hostname=agent,
            volumes=['/files', '/var/run/docker.sock'],
            ports=[repr(config['default_agent_port'])],
            host_config=cli.create_host_config(
                port_bindings={
                    config['default_agent_port']: port,
                },
                binds=[
                    work_dir + '/files:/files',
                    work_dir +
                    '/mesos_config/etc_mesos-slave:/etc/mesos-slave',
                    '/var/run/docker.sock:/var/run/docker.sock',
                ],
                privileged=True,
            ),
            environment=[
                'MESOS_PORT=' + repr(port),
                'MESOS_MASTER=zk://{0}:{1}/mesos'.format(
                    host_ip,
                    config['local_zk_port']
                ),
                'MESOS_SWITCH_USER=' + repr(config['switch_user']),
                'MESOS_CONTAINERIZERS=' + config['containers'],
                'MESOS_LOG_DIR=' + config['log_dir'],
                'MESOS_ISOLATION=' + config['isolation'],
                'MESOS_IMAGE_PROVIDERS=' + config['image_providers'],
                'MESOS_IMAGE_PROVISIONER_BACKEND={0}'.format(
                    config['image_provisioner_backend']
                ),
                'MESOS_APPC_STORE_DIR=' + config['appc_store_dir'],
                'MESOS_WORK_DIR=' + config['work_dir'],
                'MESOS_RESOURCES=' + config['resources'],
                'MESOS_MODULES=' + config['modules'],
                'MESOS_RESOURCE_ESTIMATOR=' + config['resource_estimator'],
            ],
            image=config['mesos_slave_image'],
            entrypoint='bash /files/run_mesos_slave.sh',
            detach=True,
        )
        cli.start(container=container.get('Id'))
        print_okgreen('started container %s' % agent)


#
# Run mysql
#
def run_mysql():
    # Run mysql
    remove_existing_container(config['mysql_container'])
    cli.pull(config['mysql_image'])
    container = cli.create_container(
        name=config['mysql_container'],
        hostname=config['mysql_container'],
        host_config=cli.create_host_config(
            port_bindings={
                config['default_mysql_port']: config['local_mysql_port']
            }
        ),
        environment=[
            'MYSQL_ROOT_PASSWORD=' + config['mysql_root_password'],
            'MYSQL_DATABASE=' + config['mysql_database'],
            'MYSQL_USER=' + config['mysql_user'],
            'MYSQL_PASSWORD=' + config['mysql_password'],
        ],
        image=config['mysql_image'],
        detach=True,
    )
    cli.start(container=container.get('Id'))
    print_okgreen('started container %s' % config['mysql_container'])

    print_okblue('sleep 10 secs for mysql to come up')
    time.sleep(10)


#
# Run cassandra cluster
#
def run_cassandra():
    remove_existing_container(config['cassandra_container'])
    cli.pull(config['cassandra_image'])
    container = cli.create_container(
        name=config['cassandra_container'],
        hostname=config['cassandra_container'],
        host_config=cli.create_host_config(
            port_bindings={
                config['cassandra_cql_port']: config['cassandra_cql_port'],
                config['cassandra_thrift_port']:
                    config['cassandra_thrift_port'],
            },
            binds=[
                work_dir + '/files:/files',
            ],
        ),
        environment=['MAX_HEAP_SIZE=1G', 'HEAP_NEWSIZE=256M'],
        image=config['cassandra_image'],
        detach=True,
        entrypoint='bash /files/run_cassandra_with_stratio_index.sh',
    )
    cli.start(container=container.get('Id'))
    print_okgreen('started container %s' % config['cassandra_container'])

    # Create cassandra store
    create_cassandra_store()


#
# Create cassandra store with retries
#
def create_cassandra_store():
    retry_attempts = 0
    while retry_attempts < max_retry_attempts:
        time.sleep(sleep_time_secs)
        setup_exe = cli.exec_create(
            container=config['cassandra_container'],
            cmd='/files/setup_cassandra.sh',
        )
        show_exe = cli.exec_create(
            container=config['cassandra_container'],
            cmd='cqlsh -e "describe %s"' % config['cassandra_test_db'],
        )
        # by api design, exec_start needs to be called after exec_create
        # to run 'docker exec'
        resp = cli.exec_start(exec_id=setup_exe)
        if resp is "":
            resp = cli.exec_start(exec_id=show_exe)
            if "CREATE KEYSPACE peloton_test WITH" in resp:
                print_okgreen('cassandra store is created')
                return
        print_warn('failed to create cassandra store, retrying...')
        retry_attempts += 1

    print_fail('Failed to create cassandra store after %d attempts, '
               'aborting...'
               % max_retry_attempts)
    sys.exit(1)


#
# Run peloton
#
def run_peloton(applications):
    print_okblue('docker image "uber/peloton" has to be built first '
                 'locally by running IMAGE=uber/peloton make docker')

    for app, func in APP_START_ORDER.iteritems():
        if app in applications:
            should_disable = applications[app]
            if should_disable:
                continue
        APP_START_ORDER[app]()


#
# Starts a container and waits for it to come up
#
def start_and_wait(application_name, container_name, port):
    container = cli.create_container(
        name=container_name,
        hostname=container_name,
        ports=[repr(port)],
        environment=[
            'CONFIG_DIR=config',
            'APP=%s' % application_name,
            'HTTP_PORT=' + repr(port),
            'DB_HOST=' + host_ip,
            'ELECTION_ZK_SERVERS={0}:{1}'.format(
                host_ip,
                config['local_zk_port']
            ),
            'MESOS_ZK_PATH=zk://{0}:{1}/mesos'.format(
                host_ip,
                config['local_zk_port']
            ),
            'CASSANDRA_HOSTS={0}'.format(
                host_ip,
            ),
            'ENABLE_DEBUG_LOGGING=' + config['debug'],
        ],
        host_config=cli.create_host_config(
            port_bindings={
                port: port,
            },
        ),
        # pull or build peloton image if not exists
        image=config['peloton_image'],
        detach=True,
    )
    cli.start(container=container.get('Id'))
    wait_for_up(
        container_name,
        port,
    )


#
# Run peloton resmgr app
#
def run_peloton_resmgr():
    # TODO: move docker run logic into a common function for all apps to share
    for i in range(0, config['peloton_resmgr_instance_count']):
        # to not cause port conflicts among apps, increase port by 10
        # for each instance
        port = config['peloton_resmgr_port'] + i * 10
        name = config['peloton_resmgr_container'] + repr(i)
        remove_existing_container(name)
        start_and_wait('resmgr', name, port)


#
# Run peloton hostmgr app
#
def run_peloton_hostmgr():
    for i in range(0, config['peloton_hostmgr_instance_count']):
        # to not cause port conflicts among apps, increase port
        # by 10 for each instance
        port = config['peloton_hostmgr_port'] + i * 10
        name = config['peloton_hostmgr_container'] + repr(i)
        remove_existing_container(name)
        start_and_wait('hostmgr', name, port)


#
# Run peloton jobmgr app
#
def run_peloton_jobmgr():
    for i in range(0, config['peloton_jobmgr_instance_count']):
        # to not cause port conflicts among apps, increase port by 10
        #  for each instance
        port = config['peloton_jobmgr_port'] + i * 10
        name = config['peloton_jobmgr_container'] + repr(i)
        remove_existing_container(name)
        start_and_wait('jobmgr', name, port)


#
# Run peloton placement app
#
def run_peloton_placement():
    for i in range(0, config['peloton_placement_instance_count']):
        # to not cause port conflicts among apps, increase port by 10
        # for each instance
        port = config['peloton_placement_port'] + i * 10
        name = config['peloton_placement_container'] + repr(i)
        remove_existing_container(name)
        start_and_wait('placement', name, port)


#
# Run health check for peloton apps
#
def wait_for_up(app, port):
    count = 0
    error = ''
    url = 'http://%s:%s/%s' % (
        default_host,
        port,
        healthcheck_path,
    )
    while count < max_retry_attempts:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                print_okgreen('started %s' % app)
                return
        except Exception, e:
            print_warn('app %s is not up yet, retrying...' % app)
            error = str(e)
            time.sleep(sleep_time_secs)
            count += 1

    raise Exception('failed to start %s on %d after %d attempts, err: %s' %
                    (
                        app,
                        port,
                        max_retry_attempts,
                        error,
                    )
                    )


#
# Set up a personal cluster
#
def setup(applications={}, enable_peloton=False):
    run_cassandra()
    run_mysql()
    run_mesos()

    if enable_peloton:
        run_peloton(
            applications
        )


#
# Tear down a personal cluster
# TODO (wu): use docker labels when launching containers
#            and then remove all containers with that label
def teardown():
    teardown_mesos()

    remove_existing_container(config['mysql_container'])

    remove_existing_container(config['cassandra_container'])

    for i in range(0, config['peloton_resmgr_instance_count']):
        name = config['peloton_resmgr_container'] + repr(i)
        remove_existing_container(name)

    for i in range(0, config['peloton_hostmgr_instance_count']):
        name = config['peloton_hostmgr_container'] + repr(i)
        remove_existing_container(name)

    for i in range(0, config['peloton_jobmgr_instance_count']):
        name = config['peloton_jobmgr_container'] + repr(i)
        remove_existing_container(name)

    for i in range(0, config['peloton_placement_instance_count']):
        name = config['peloton_placement_container'] + repr(i)
        remove_existing_container(name)


def parse_arguments():
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by %s on %s.
  Copyright Uber Compute Platform. All rights reserved.

USAGE
''' % (program_shortdesc, __author__, str(__date__))
    # Setup argument parser
    parser = ArgumentParser(
        description=program_license,
        formatter_class=RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(help='command help', dest='command')
    # Subparser for the 'setup' command
    parser_setup = subparsers.add_parser(
        'setup',
        help='set up a personal cluster')
    parser_setup.add_argument(
        "-a",
        "--enable-peloton",
        dest="enable_peloton",
        action='store_true',
        default=False,
        help="enable peloton",
    )
    parser_setup.add_argument(
        "--no-resmgr",
        dest="disable_peloton_resmgr",
        action='store_true',
        default=False,
        help="disable peloton resmgr app"
    )
    parser_setup.add_argument(
        "--no-hostmgr",
        dest="disable_peloton_hostmgr",
        action='store_true',
        default=False,
        help="disable peloton hostmgr app"
    )
    parser_setup.add_argument(
        "--no-jobmgr",
        dest="disable_peloton_jobmgr",
        action='store_true',
        default=False,
        help="disable peloton jobmgr app"
    )
    parser_setup.add_argument(
        "--no-placement",
        dest="disable_peloton_placement",
        action='store_true',
        default=False,
        help="disable peloton placement engine app"
    )
    # Subparser for the 'teardown' command
    subparsers.add_parser('teardown', help='tear down a personal cluster')
    # Process arguments
    return parser.parse_args()


class App:
    """
    Represents the peloton apps
    """
    RESOURCE_MANAGER = 1
    HOST_MANAGER = 2
    PLACEMENT_ENGINE = 3
    JOB_MANAGER = 4


# Defines the order in which the apps are started
# NB: HOST_MANAGER is tied to database migrations so should be started first
APP_START_ORDER = OrderedDict([
    (App.HOST_MANAGER, run_peloton_hostmgr),
    (App.RESOURCE_MANAGER, run_peloton_resmgr),
    (App.PLACEMENT_ENGINE, run_peloton_placement),
    (App.JOB_MANAGER, run_peloton_jobmgr)]
)


def main():
    args = parse_arguments()

    command = args.command

    if command == 'setup':
        applications = {
            App.HOST_MANAGER: args.disable_peloton_hostmgr,
            App.RESOURCE_MANAGER: args.disable_peloton_resmgr,
            App.PLACEMENT_ENGINE: args.disable_peloton_placement,
            App.JOB_MANAGER: args.disable_peloton_jobmgr
        }

        setup(
            enable_peloton=args.enable_peloton,
            applications=applications
        )
    elif command == 'teardown':
        teardown()
    else:
        # Should never get here.  argparser should prevent it.
        print_fail('Unknown command: %s' % command)
        return 1


if __name__ == "__main__":
    main()
