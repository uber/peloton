#!/usr/bin/env python
'''
 -- Locally run and manage a personal cluster in containers.

This script can be used to manage (setup, teardown) a personal Mesos cluster and Mysql etc in containers.
Once a personal cluster is running you can run Peloton master against it.

@author:     wu

@copyright:  2016 Uber Compute Platform. All rights reserved.

@license:    license

@contact:    wu@uber.com
'''

import os
import sys
import time
import yaml
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from docker import Client

__date__ = '2016-12-08'
__author__ = 'wu'


#
# Get eth0 ip of the host
# TODO: see if we can do better than running ipconfig/ifconfig
#
def get_host_ip():
    uname = os.uname()[0]
    if uname == "Darwin":
        ip = os.popen('ipconfig getifaddr en0').read()
    else:
        ip = os.popen('ifconfig eth0 | grep "inet addr:" | cut -d: -f2 | awk "{ print $1}"').read()
    return ip.strip()


#
# Load configs from file
#
def load_config():
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


host_ip = get_host_ip()
cli = Client(base_url='unix://var/run/docker.sock')
work_dir = os.path.dirname(os.path.abspath(__file__))
config = load_config()


#
# Force remove container by name
#
def remove_existing_container(name):
    try:
        cli.remove_container(name, force=True)
        print 'removed container %s' % name
    except Exception, e:
        print 'Error removing container %s : %s, skipping' % (name, e)
        pass


#
# Run mesos cluster
#
def run_mesos():
    # Run zk
    remove_existing_container(config['zk_container'])
    cli.pull(config['zk_image'])
    container = cli.create_container(
        name=config['zk_container'],
        volumes=['/scripts'],
        host_config=cli.create_host_config(
            port_bindings={
                config['default_zk_port']: config['local_zk_port'],
            },
            binds=[
                work_dir + '/scripts:/scripts',
            ]),
        image=config['zk_image'],
        detach=True
    )
    cli.start(container=container.get('Id'))
    print 'started container %s' % config['zk_container']

    # TODO: add retry
    print 'sleep 20 secs for zk to come up'
    time.sleep(20)

    # Create zk nodes
    exe = cli.exec_create(container=config['zk_container'], cmd='/scripts/setup_zk.sh')
    cli.exec_start(exec_id=exe)

    # Run mesos master
    remove_existing_container(config['mesos_master_container'])
    cli.pull(config['mesos_master_image'])
    container = cli.create_container(
        name=config['mesos_master_container'],
        volumes=['/scripts'],
        ports=[repr(config['master_port'])],
        host_config=cli.create_host_config(
            port_bindings={
                config['master_port']: config['master_port'],
            },
            binds=[
                work_dir + '/scripts:/scripts',
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
            'MESOS_ADVERTISE_IP={}'.format(host_ip),
        ],
        image=config['mesos_master_image'],
        entrypoint='bash /scripts/run_mesos_master.sh',
        detach=True,
    )
    cli.start(container=container.get('Id'))
    print 'started container %s' % config['mesos_master_container']

    # Run mesos slaves
    cli.pull(config['mesos_slave_image'])
    for i in range(0, config['num_agents']):
        agent = config['mesos_agent_container'] + repr(i)
        port = config['agent_port'] + i
        remove_existing_container(agent)
        container = cli.create_container(
            name=agent,
            volumes=['/scripts', '/var/run/docker.sock'],
            ports=[repr(config['agent_port'])],
            host_config=cli.create_host_config(
                port_bindings={
                    config['agent_port']: port,
                },
                binds=[
                    work_dir + '/scripts:/scripts',
                    work_dir + '/mesos_config/etc_mesos-slave:/etc/mesos-slave',
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
            entrypoint='bash /scripts/run_mesos_slave.sh',
            detach=True,
        )
        cli.start(container=container.get('Id'))
        print 'started container %s' % agent


#
# Run mysql
#
def run_mysql():
    # Run mysql
    remove_existing_container(config['mysql_container'])
    cli.pull(config['mysql_image'])
    container = cli.create_container(
        name=config['mysql_container'],
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
    print 'started container %s' % config['mysql_container']

    print 'sleep 10 secs for mysql to come up'
    time.sleep(10)


#
# Run cassandra cluster
#
def run_cassandra():
    remove_existing_container(config['cassandra_container'])
    cli.pull(config['cassandra_image'])
    container = cli.create_container(
        name=config['cassandra_container'],
        host_config=cli.create_host_config(
            port_bindings={
                config['cassandra_cql_port']: config['cassandra_cql_port'],
                config['cassandra_thrift_port']: config['cassandra_thrift_port'],
            }
        ),
        image=config['cassandra_image'],
        detach=True,
    )
    cli.start(container=container.get('Id'))
    print 'started container %s' % config['cassandra_container']


#
# Run peloton (@wu: This is still experimental)
#
def run_peloton():
    # On Mac, peloton can only be accessed by 'localhost:<port>'
    # from inside a container, there is no such issue with Linux.
    # TODO: update the docker run mechanism once T664460 is resolved
    cli.pull(config['peloton_image'])
    for i in range(0, config['peloton_master_instance_count']):
        port = config['peloton_master_port'] + i
        name = config['peloton_container'] + repr(i)
        remove_existing_container(name)
        container = cli.create_container(
            name=name,
            environment=[
                'MASTER_PORT=' + repr(port)
            ],
            host_config=cli.create_host_config(
                network_mode='host',
            ),
            # pull or build peloton image if not exists
            image=config['peloton_image'],
            detach=True,
        )
        cli.start(container=container.get('Id'))
        print 'started container %s' % name


#
# Set up a personal cluster
#
def setup(enable_mesos=True, enable_mysql=True, enable_cassandra=True, enable_peloton=False):
    if enable_mysql:
        run_mysql()

    if enable_mesos:
        run_mesos()

    if enable_cassandra:
        run_cassandra()

    if enable_peloton:
        run_peloton()


#
# Tear down a personal cluster
#
def teardown():
    remove_existing_container(config['zk_container'])

    remove_existing_container(config['mesos_master_container'])

    for i in range(0, config['num_agents']):
        agent = config['mesos_agent_container'] + repr(i)
        remove_existing_container(agent)

    remove_existing_container(config['mysql_container'])

    remove_existing_container(config['cassandra_container'])

    for i in range(0, config['peloton_master_instance_count']):
        name = config['peloton_container'] + repr(i)
        remove_existing_container(name)


def main(argv):
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by %s on %s.
  Copyright Uber Compute Platform. All rights reserved.

USAGE
''' % (program_shortdesc, __author__, str(__date__))

    # Setup argument parser
    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)

    subparsers = parser.add_subparsers(help='command help', dest='command')

    # Subparser for the 'setup' command
    parser_setup = subparsers.add_parser('setup', help='set up a personal cluster')
    parser_setup.add_argument(
        "-ep",
        "--enable_peloton",
        dest="enable_peloton",
        action='store_true',
        default=False,
        help="enable peloton"
    )

    # Subparser for the 'teardown' command
    subparsers.add_parser('teardown', help='tear down a personal cluster')

    # Process arguments
    args = parser.parse_args()

    command = args.command

    if command == 'setup':
        setup(
            enable_peloton=args.enable_peloton,
        )
    elif command == 'teardown':
        teardown()
    else:
        # Should never get here.  argparser should prevent it.
        print 'Unknown command: %s' % command
        return 1


if __name__ == "__main__":
    main(sys.argv[1:])
