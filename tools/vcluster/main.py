#!/usr/bin/env python

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from vcluster import (
    VCluster,
    cassandra_operation,
)

DATE = '2017-09-13'
AUTHOR = 'Chunyang Shen'

LOGO = '''
  Created by %s on %s.
  Copyright Uber Compute Platform. All rights reserved.

USAGE
'''


def parse_arguments():
    program_license = LOGO % (AUTHOR, DATE)

    parser = ArgumentParser(
        description=program_license,
        formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        '-z',
        '--zookeeper-server',
        dest='zookeeper_server',
        help='the DNS of zookeeper server of the physical Peloton cluster',
    )

    parser.add_argument(
        '-n',
        '--name',
        dest='label_name',
        help='the name of the vcluster',
    )

    parser.add_argument(
        '-p',
        '--resource-pool',
        dest='respool_path',
        help='the path of the resource pool',
    )

    subparsers = parser.add_subparsers(help='command help', dest='command')

    # Subparser for the 'mesos' command
    parser_mesos = subparsers.add_parser(
        'mesos',
        help='set up a virtual cluster with Mesos master and Mesos slave')

    parser_mesos.add_argument(
        '-s',
        '--agent-number',
        type=int,
        dest='agent_number',
        help='the number of Mesos agent in the vcluster',
    )

    # Subparser for the 'setup' command
    parser_peloton = subparsers.add_parser(
        'setup',
        help='set up a virtual cluster')

    parser_peloton.add_argument(
        '-s',
        '--agent-number',
        type=int,
        dest='agent_number',
        help='the number of Mesos agent in the vcluster',
    )

    # Subparser for the 'shutdown' command
    subparsers.add_parser(
        'teardown',
        help='shut down a virtual cluster')

    # Subparser for the 'shutdown' command
    parser_peloton = subparsers.add_parser(
        'peloton',
        help='start peloton on a Mesos cluster')

    parser_peloton.add_argument(
        '-k',
        '--zk',
        dest='zk',
        help='the host of virtual zk',
    )

    # Subparser for the 'shutdown' command
    parser_cassandra = subparsers.add_parser(
        'cassandra',
        help='cassandra keyspace creation and migration')
    parser_cassandra.add_argument(
        '-o',
        '--option',
        dest='option',
        help='option of action',
    )

    return parser.parse_args()


def main():
    args = parse_arguments()
    vcluster = VCluster(
        args.label_name,
        args.zookeeper_server,
        args.respool_path
    )

    command = args.command

    if command == 'mesos':
        agent_number = args.agent_number
        vcluster.start_mesos(agent_number)

    elif command == 'peloton':
        zk = args.zk.split(':')
        if len(zk) != 2:
            raise Exception("Invalid zk")
        vcluster.start_peloton(zk[0], zk[1])

    elif command == 'setup':
        agent_number = args.agent_number
        vcluster.start_all(agent_number)

    elif command == 'teardown':
        vcluster.teardown()

    elif command == 'cassandra':
        option = args.option
        if option == 'up':
            cassandra_operation(
                create=True, keyspace=args.label_name)
        elif option == 'drop':
            cassandra_operation(
                create=False, keyspace=args.label_name)


if __name__ == "__main__":
    main()
