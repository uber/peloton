#!/usr/bin/env python

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from performance_test_client import PerformanceTestClient

"""
This is a script for launch a benchmark job with the given zookeeper,
instance number and sleep time.

"""


def parse_arguments():
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        "-z",
        "--zookeeper-server",
        dest="zookeeper_server",
        help="the DNS of zookeeper server of the physical Peloton cluster",
    )

    parser.add_argument(
        "-n",
        "--instance_num",
        type=int,
        dest="instance_num",
        help="number of instance",
    )

    parser.add_argument(
        "-t",
        "--sleep-time",
        type=int,
        dest="sleep_time",
        help="sleep time in seconds for each task",
    )

    parser.add_argument(
        "-i",
        "--instance-config",
        type=lambda s: s.lower() in ["true", "t", "yes", "1"],
        default=False,
        dest="instance_config",
        help="with unique insstance config or not",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()
    pf_client = PerformanceTestClient(args.zookeeper_server)

    succeeded, start_time, completion_time = pf_client.run_benchmark(
        args.instance_num, args.sleep_time, args.instance_config
    )
    resp = {
        "Succeeded": succeeded,
        "Instance": args.instance_num,
        "Task duration (s)": args.sleep_time,
        "Use instance config": args.instance_config,
    }
    if succeeded:
        resp.update(
            {
                "Time to start(s)": start_time,
                "Total Execution Time(s)": completion_time,
            }
        )

    print(resp)


if __name__ == "__main__":
    main()
