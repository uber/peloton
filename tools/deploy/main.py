#!/usr/bin/env python

import argparse

from cluster import Cluster


def parse_args():
    parser = argparse.ArgumentParser(description="Script to deploy Peloton")
    parser.add_argument(
        "-c",
        "--config",
        required=True,
        help="The cluster config file to deploy Peloton",
    )
    parser.add_argument(
        "-f",
        "--force",
        required=False,
        action="store_true",
        help="Force the upgrade without confirmation",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        required=False,
        action="store_true",
        help="Print more verbose information",
    )

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    cluster = Cluster.load(args.config)

    if cluster.update(args.force, args.verbose):
        print(
            "Successfully updated cluster %s to %s"
            % (cluster.name, cluster.version)
        )
    else:
        print("Failed to update cluster %s" % cluster.name)


if __name__ == "__main__":
    main()
