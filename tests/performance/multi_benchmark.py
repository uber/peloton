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
import datetime
import pandas as pd
import os
import json

from performance_test_client import (
    PerformanceTestClient,
    ResPoolNotFoundException,
    JobCreateFailedException,
)


INSTANCE_NUM = [10000, 50000]
SLEEP_TIME_SEC = [10, 60]
INSTANCE_CONFIG = [True, False]


def main():

    file_name = '.vcluster'

    try:
        cluster_config = json.loads(open(file_name).read())
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
                    'Task Number': instance_num,
                    'Sleep Time(s)': sleep_time,
                    'Use Instance Config': instance_config,
                    'Succeeded': succeeded,
                    'Peloton Version': peloton_version,
                    'Mesos Slave Number': agent_num,
                }
                if succeeded:
                    record.update({
                        'Start time(s)': start,
                        'Execution Time(s)': completion,
                    })

                print(record)
                records.append(record)

    df = pd.DataFrame(records)
    print(df)

    res_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'PERF_RES'
    )
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)

    exp_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
    file_name = os.path.join(res_dir, exp_time + '.csv')

    df.to_csv(file_name, sep='\t')


if __name__ == "__main__":
    main()
