#!/usr/bin/env python
"""
 -- Locally run and manage a personal cluster in containers.

This script can be used to manage (setup, teardown) a personal
Mesos cluster etc in containers, optionally Peloton
master or apps can be specified to run in containers as well.

@copyright:  2019 Uber Compute Platform. All rights reserved.

@license:    license

@contact:    peloton-dev@uber.com
"""

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import minicluster
import print_utils
import utils

__date__ = "2016-12-08"
__author__ = "wu"


def parse_arguments():
    program_shortdesc = __import__("__main__").__doc__.split("\n")[1]
    program_license = """%s

  Created by %s on %s.
  Copyright Uber Compute Platform. All rights reserved.

USAGE
""" % (
        program_shortdesc,
        __author__,
        str(__date__),
    )
    # Setup argument parser
    parser = ArgumentParser(
        description=program_license,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--num-peloton-instance",
        dest="num_peloton_instance",
        type=int,
        default=1,
        help="customize number of peloton instance",
    )

    subparsers = parser.add_subparsers(help="command help", dest="command")
    # Subparser for the 'setup' command
    parser_setup = subparsers.add_parser(
        "setup", help="set up a personal cluster"
    )
    parser_setup.add_argument(
        "--k8s",
        dest="enable_k8s",
        action="store_true",
        default=False,
        help="enable k8s setup",
    )
    parser_setup.add_argument(
        "--no-mesos",
        dest="disable_mesos",
        action="store_true",
        default=False,
        help="disable mesos setup",
    )
    parser_setup.add_argument(
        "--zk_url",
        dest="zk_url",
        action="store",
        type=str,
        default=None,
        help="zk URL when pointing to a pre-existing zk",
    )
    parser_setup.add_argument(
        "-a",
        "--enable-peloton",
        dest="enable_peloton",
        action="store_true",
        default=False,
        help="enable peloton",
    )
    parser_setup.add_argument(
        "--no-resmgr",
        dest="disable_peloton_resmgr",
        action="store_true",
        default=False,
        help="disable peloton resmgr app",
    )
    parser_setup.add_argument(
        "--no-hostmgr",
        dest="disable_peloton_hostmgr",
        action="store_true",
        default=False,
        help="disable peloton hostmgr app",
    )
    parser_setup.add_argument(
        "--no-jobmgr",
        dest="disable_peloton_jobmgr",
        action="store_true",
        default=False,
        help="disable peloton jobmgr app",
    )
    parser_setup.add_argument(
        "--no-placement",
        dest="disable_peloton_placement",
        action="store_true",
        default=False,
        help="disable peloton placement engine app",
    )
    parser_setup.add_argument(
        "--no-archiver",
        dest="disable_peloton_archiver",
        action="store_true",
        default=False,
        help="disable peloton archiver app",
    )
    parser_setup.add_argument(
        "--no-aurorabridge",
        dest="disable_peloton_aurorabridge",
        action="store_true",
        default=False,
        help="disable peloton aurora bridge app",
    )
    parser_setup.add_argument(
        "--no-apiserver",
        dest="disable_peloton_apiserver",
        action="store_true",
        default=False,
        help="disable peloton api server app",
    )
    parser_setup.add_argument(
        "--no-mockcqos",
        dest="disable_peloton_mockcqos",
        action="store_true",
        default=False,
        help="disable peloton mock cqos app",
    )
    parser_setup.add_argument(
        "--use-host-pool",
        dest="use_host_pool",
        action="store_true",
        default=False,
        help="Use host pool for placement",
    )

    # Subparser for the 'teardown' command
    subparsers.add_parser("teardown", help="tear down a personal cluster")

    # Process arguments
    return parser.parse_args()


def customize_config(config, num_peloton_instance):
    config['peloton_resmgr_instance_count'] = num_peloton_instance
    config['peloton_hostmgr_instance_count'] = num_peloton_instance
    config['peloton_jobmgr_instance_count'] = num_peloton_instance
    return config


def main():
    args = parse_arguments()
    config = utils.default_config()
    config = customize_config(config, args.num_peloton_instance)

    command = args.command

    if command == "setup":
        disabled_applications = {
            minicluster.HOST_MANAGER: args.disable_peloton_hostmgr,
            minicluster.RESOURCE_MANAGER: args.disable_peloton_resmgr,
            minicluster.PLACEMENT_ENGINE: args.disable_peloton_placement,
            minicluster.JOB_MANAGER: args.disable_peloton_jobmgr,
            minicluster.ARCHIVER: args.disable_peloton_archiver,
            minicluster.AURORABRIDGE: args.disable_peloton_aurorabridge,
            minicluster.MOCK_CQOS: args.disable_peloton_mockcqos,
        }

        cluster = minicluster.Minicluster(
            config,
            disable_mesos=args.disable_mesos,
            enable_k8s=args.enable_k8s,
            enable_peloton=args.enable_peloton,
            disabled_applications=disabled_applications,
            use_host_pool=args.use_host_pool,
            zk_url=args.zk_url,
        )
        cluster.setup()
    elif command == "teardown":
        cluster = minicluster.Minicluster(config)
        cluster.teardown()
    else:
        # Should never get here.  argparser should prevent it.
        print_utils.fail("Unknown command: %s" % command)
        return 1


if __name__ == "__main__":
    main()
