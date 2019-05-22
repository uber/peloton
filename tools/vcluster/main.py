#!/usr/bin/env python
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import os

from config_generator import load_config
from vcluster import VCluster

DATE = "2017-09-13"
AUTHOR = "Chunyang Shen"

LOGO = """
  Created by %s on %s.
  Copyright Uber Compute Platform. All rights reserved.

USAGE
"""


def parse_arguments():
    program_license = LOGO % (AUTHOR, DATE)

    parser = ArgumentParser(
        description=program_license,
        formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-c",
        "--config-file",
        dest="config_file",
        help="Path to vcluster config file. Default is config/default.yaml",
    )

    parser.add_argument(
        "-z",
        "--zookeeper-server",
        dest="zookeeper_server",
        help="the DNS of zookeeper server of the physical Peloton cluster",
    )

    parser.add_argument(
        "-n", "--name", dest="label_name", help="the name of the vcluster"
    )

    parser.add_argument(
        "-p",
        "--resource-pool",
        dest="respool_path",
        help="the path of the resource pool",
    )

    parser.add_argument(
        "--auth-type",
        dest="auth_type",
        help="auth type of the physical Peloton cluster",
    )

    parser.add_argument(
        "--auth-config-file",
        dest="auth_config_file",
        help="auth config file used to talk to the physical Peloton cluster",
    )

    subparsers = parser.add_subparsers(help="command help", dest="command")

    # Subparser for the 'mesos' command
    parser_mesos = subparsers.add_parser(
        "mesos",
        help="set up a virtual cluster with Mesos master and Mesos slave",
    )

    parser_mesos.add_argument(
        "-s",
        "--agent-number",
        type=int,
        dest="agent_number",
        help="the number of Mesos agent in the vcluster",
    )

    # Subparser for the 'mesos-slave' command
    parser_mesos_slave = subparsers.add_parser(
        "mesos-slave", help="adding Mesos slaves giving a zookeeper"
    )

    parser_mesos_slave.add_argument(
        "-s",
        "--agent-number",
        type=int,
        dest="agent_number",
        help="the number of Mesos slaves add into the vcluster",
    )

    parser_mesos_slave.add_argument(
        "-k", "--zk", dest="zk", help="the host of virtual zk"
    )

    # Subparser for the 'mesos-master' command
    parser_mesos_slave = subparsers.add_parser(
        "mesos-master",
        help="set up a virtual cluster with Mesos master and Mesos slave",
    )

    parser_mesos_slave.add_argument(
        "-k", "--zk", dest="zk", help="the host of virtual zk"
    )

    # Subparser for the 'setup' command
    parser_setup = subparsers.add_parser(
        "setup", help="set up a virtual cluster"
    )

    parser_setup.add_argument(
        "-v",
        "--peloton-version",
        nargs="?",
        dest="peloton_version",
        help="The image version for Peloton",
    )

    parser_setup.add_argument(
        "-c",
        "--peloton-app-config",
        dest="peloton_apps_config_path",
        help="the path of the peloton apps config",
    )

    parser_setup.add_argument(
        "-i",
        "--peloton-image",
        dest="peloton_image",
        default=None,
        help="Docker image to use for Peloton. "
        + "If specified, overrides option -v",
    )

    parser_setup.add_argument(
        "-s",
        "--agent-number",
        type=int,
        dest="agent_number",
        help="the number of Mesos agent in the vcluster",
    )

    parser_setup.add_argument(
        "--no-respool",
        action="store_true",
        dest="skip_respool",
        help="If set, default resource-pool will not be created",
    )
    parser_setup.add_argument(
        "--clean",
        action="store_true",
        dest="clean_setup",
        help="Clean up old instance(s) of vcluster before creating a new one",
    )

    # Subparser for the 'teardown' command
    parser_teardown = subparsers.add_parser(
        "teardown", help="shut down a virtual cluster"
    )
    parser_teardown.add_argument(
        "-o", "--option", dest="option", help="option of action"
    )
    parser_teardown.add_argument(
        "--remove",
        action="store_true",
        dest="remove",
        help="Delete Peloton jobs as part of tearing down vcluster",
    )
    # Subparser for the 'peloton' command
    parser_peloton = subparsers.add_parser(
        "peloton", help="start peloton on a Mesos cluster"
    )

    parser_peloton.add_argument(
        "-k", "--zk", dest="zk", help="the host of virtual zk"
    )

    parser_peloton.add_argument(
        "-s",
        "--agent-number",
        type=int,
        dest="agent_number",
        help="the number of Mesos agent in the vcluster",
    )

    parser_peloton.add_argument(
        "-v",
        "--peloton-version",
        nargs="?",
        dest="peloton_version",
        help="The image version for Peloton",
    )

    parser_peloton.add_argument(
        "-i",
        "--peloton-image",
        dest="peloton_image",
        default=None,
        help="Docker image to use for Peloton. "
        + "If specified, overrides option -v",
    )

    subparsers.add_parser(
        "parameters", help="get the parameters of the vCluster"
    )

    return parser.parse_args()


def main():
    args = parse_arguments()
    if not args.config_file:
        args.config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "config",
            "default.yaml",
        )
    config = load_config(args.config_file)

    vcluster = VCluster(
        config,
        args.label_name,
        args.zookeeper_server,
        args.respool_path,
        auth_type=args.auth_type,
        auth_file=args.auth_config_file,
    )

    command = args.command

    if command == "mesos":
        agent_number = args.agent_number
        vcluster.start_mesos(agent_number)

    elif command == "mesos-slave":
        agent_number = args.agent_number
        zk = args.zk.split(":")
        if len(zk) != 2:
            raise Exception("Invalid zk")
        vcluster.start_mesos_slave(args.zk, agent_number)

    elif command == "mesos-master":
        zk = args.zk.split(":")
        if len(zk) != 2:
            raise Exception("Invalid zk")
        vcluster.start_mesos_master(args.zk)

    elif command == "peloton":
        zk = args.zk.split(":")
        if len(zk) != 2:
            raise Exception("Invalid zk")
        vcluster.start_peloton(
            args.zk,
            args.agent_number,
            args.peloton_version,
            skip_respool=args.skip_respool,
            peloton_image=args.peloton_image,
            peloton_apps_config=args.peloton_apps_config_path,
        )

    elif command == "setup":
        agent_number = args.agent_number
        peloton_version = args.peloton_version
        if args.clean_setup:
            vcluster.teardown()
        vcluster.start_all(
            agent_number,
            peloton_version,
            skip_respool=args.skip_respool,
            peloton_image=args.peloton_image,
            peloton_apps_config=args.peloton_apps_config_path,
        )

    elif command == "teardown":
        option = args.option
        if option == "slave":
            vcluster.teardown_slave(remove=args.remove)
        elif option == "peloton":
            vcluster.teardown_peloton(remove=args.remove)
        else:
            vcluster.teardown(remove=args.remove)


if __name__ == "__main__":
    main()
