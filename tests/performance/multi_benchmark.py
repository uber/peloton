#!/usr/bin/env python
"""
This is a script for launch different benchmark jobs with:
    Instance number: 10k, 50k
    Sleep time: 10s, 300s,
    Instance config: true, false
on the cluster configured in the file '.vcluster'. The file
'.vcluster' contains keys 'Zookeeper', 'Peloton Version' and
'Mesos Slave Number'.

Details are in
https://code.uberinternal.com/w/projects/peloton/performance-test/
how to run this script.
"""
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import pandas as pd
import os
import sys
import json

from performance_test_client import (
    PerformanceTestClient,
    ResPoolNotFoundException,
    JobCreateFailedException,
)


INSTANCE_NUM = [10000, 50000]
SLEEP_TIME_SEC = [10, 60]
INSTANCE_CONFIG = [True, False]


def parse_arguments(args):

    parser = ArgumentParser(
        description='',
        formatter_class=RawDescriptionHelpFormatter)

    # Input the vCluster config file
    parser.add_argument(
        '-i',
        '--input-file',
        dest='input_file',
        help='the input config file of vCluster',
    )

    # Output the performance data
    parser.add_argument(
        '-o',
        '--output-file',
        dest='output_file',
        help='the output file to store perf result',
    )
    return parser.parse_args(args)


def main():
    args = parse_arguments(sys.argv[1:])

    vcluster_config = args.input_file
    output_file = args.output_file

    try:
        cluster_config = json.loads(open(vcluster_config).read())
        agent_num = int(cluster_config['Mesos Slave Number'])
        zkserver = cluster_config['Zookeeper']
        peloton_version = cluster_config['Peloton Version']
        pf_client = PerformanceTestClient(zkserver, agent_num, peloton_version)
    except IOError:
        print "Can't find the vcluster config file."
        return
    except ValueError:
        print "This vcluster config is not a valid JSON file."
        return
    except ResPoolNotFoundException:
        print "Can't find Respool for this Peloton Cluster."
        return
    peloton_version = cluster_config['Peloton Version']
    records = []

    for instance_num in INSTANCE_NUM:
        for sleep_time in SLEEP_TIME_SEC:
            for instance_config in INSTANCE_CONFIG:
                try:
                    succeeded, start, completion = pf_client.run_benchmark(
                        instance_num,
                        sleep_time,
                        instance_config,
                    )
                except JobCreateFailedException:
                    print "TaskNum %s && SleepTime %s && InstanceConfig %s" % (
                        instance_num, sleep_time, instance_config,
                    ) + "Test launch failed!"
                    continue
                record = {
                    'TaskNum': instance_num,
                    'Sleep(s)': sleep_time,
                    'UseInsConf': instance_config,
                    'Version': peloton_version,
                    'Cores': agent_num,
                }
                if succeeded:
                    record.update({
                        'Start(s)': start,
                        'Exec(s)': completion,
                    })

                print(record)
                records.append(record)

    df = pd.DataFrame(
        records,
        columns=['Cores', 'TaskNum', 'Sleep(s)',
                 'UseInsConf', 'Version', 'Start(s)',
                 'Exec(s)']
    )
    print(df)

    res_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'PERF_RES'
    )
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    output = os.path.join(res_dir, output_file)

    df.to_csv(output, sep='\t')


if __name__ == "__main__":
    main()
