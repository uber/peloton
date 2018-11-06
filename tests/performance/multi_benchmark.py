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
from collections import Counter

import json
import os
import pandas as pd
import sys
import threading
import time

from performance_test_client import (
    PerformanceTestClient,
    ResPoolNotFoundException,
    JobCreateFailedException,
)


NUM_TASKS = [10000, 50000]
SLEEP_TIME_SEC = [10, 60]
USE_INSTANCE_CONFIG = [True, False]

stats_keys = ['CREATE', 'CREATEFAILS', 'GET', 'GETFAILS']


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
    res_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'PERF_RES'
    )
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)
    output = os.path.join(res_dir, output_file)
    records = []

    for num_tasks in NUM_TASKS:
        for sleep_time in SLEEP_TIME_SEC:
            for instance_config in USE_INSTANCE_CONFIG:
                try:
                    job_id = pf_client.create_job(
                        num_tasks, sleep_time, instance_config,
                    )
                    succeeded, start, completion = pf_client.monitoring(job_id)
                except JobCreateFailedException:
                    print "TaskNum %s && SleepTime %s && InstanceConfig %s" % (
                        num_tasks, sleep_time, instance_config,
                    ) + "Test launch failed!"
                    continue
                record = {
                    'TaskNum': num_tasks,
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

    df.to_csv(output, sep='\t')

    t = PerformanceTest(pf_client, output)

    # create 5000 jobs in 100 threads and count num of job.Get() that
    # can be done in 300sec
    t.perf_test_job_create_get(
        num_threads=100, jobs_per_thread=50, num_tasks=10)

    # create 1 job with 50k tasks and count num of job.Get() that can be done
    # by 100 threads in 300sec
    t.perf_test_job_create_get(
        num_threads=100, num_tasks=50000, get_only=True)

    # create 1 job with 50k instance configs and count num of job.Get() that
    # can be done by 1 thread in 300sec
    # use 1 thread because using multiple threads causes OOM on vcluster jobmgr
    t.perf_test_job_create_get(
        num_threads=1, num_tasks=50000,
        use_instance_config=True, get_only=True)

    t.dump_records_to_file()


class perfCounter():
    def __init__(self):
        self.counter = Counter()
        self.lock = threading.Lock()

    def inc(self, key):
        if key not in stats_keys:
            return
        with self.lock:
            self.counter[key] += 1

    def get(self):
        with self.lock:
            return self.counter


class PerformanceTest ():
    def __init__(self, client, output_file):
        self.client = client
        self.output_file = output_file
        self.records = []

    def perf_test_job_create_get(self, num_threads=10, jobs_per_thread=5,
                                 num_tasks=10, sleep_time=10,
                                 use_instance_config=False, get_only=False):
        """
        perf_test_job_create_get can be used for testing job Create + Get or
        only job Get at scale. The test starts num_threads number of jobWorker
        threads which either uses a pre created job, or creates a batch of jobs
        indicated by jobs_per_thread. The test waits for 300sec to allow each
        thread to perform as many job.Gets as possible and records cumulative
        success/failure counters for creates and gets. All the stats are
        appended to the records list which will be later used to dump all stats
        """

        self.counter = perfCounter()
        threads = []
        stopper = threading.Event()

        if get_only:
            try:
                job_id = self.client.create_job(
                    num_tasks, sleep_time, use_instance_config)
                self.counter.inc('CREATE')
            except JobCreateFailedException:
                print "TaskNum %s && SleepTime %s && InstanceConfig %s" % (
                    num_tasks, sleep_time, use_instance_config,
                ) + "Test launch failed!"
                self.counter.inc('CREATEFAILS')
                return
        else:
            job_id = None

        for i in xrange(num_threads):
            t = jobWorker(self.client, num_tasks, jobs_per_thread, stopper,
                          use_instance_config, job_id, sleep_time,
                          self.counter)
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # let all threads run for 300 sec
        time.sleep(300)

        # signal all threads to stop
        stopper.set()

        for t in threads:
            t.join(60)

        counts = self.counter.get()

        record = {
            'TaskNum': num_tasks,
            'Sleep(s)': sleep_time,
            'UseInsConf': use_instance_config,
            'Creates': counts['CREATE'],
            'CreateFails': counts['CREATEFAILS'],
            'Gets': counts['GET'],
            'GetFails': counts['GETFAILS'],
        }
        self.records.append(record)

    def dump_records_to_file(self):
        df = pd.DataFrame(
            self.records,
            columns=['TaskNum', 'Sleep(s)',
                     'UseInsConf', 'Creates',
                     'CreateFails', 'Gets', 'GetFails']
        )
        print(df)
        # with open(self.output_file, 'a') as f:
        #    df.to_csv(f, header=False)


class jobWorker (threading.Thread):
    """
    jobWorker is a worker class that can be used to perform job.Create() and
    job.Get() at scale
    """
    def __init__(
            self, client, num_tasks, jobs_per_thread, stopper,
            use_instance_config=False, job_id=None, sleep_time=10,
            counter=None):
        threading.Thread.__init__(self)
        self.client = client
        self.num_tasks = num_tasks
        self.jobs_per_thread = jobs_per_thread
        self.stopper = stopper
        self.use_instance_config = use_instance_config
        self.job_id = job_id
        self.sleep_time = sleep_time
        self.counter = counter

    def run(self):
        job_ids = []
        if self.job_id is None:
            # create job then run job.Get()
            for i in xrange(self.jobs_per_thread):
                try:
                    job_id = self.client.create_job(
                        self.num_tasks, self.sleep_time,
                        self.use_instance_config)
                    self.counter.inc('CREATE')
                    job_ids.append(job_id)
                except JobCreateFailedException:
                    print "TaskNum %s && SleepTime %s && InstanceConfig %s" % (
                        self.num_tasks, self.sleep_time,
                        self.use_instance_config) + "Test launch failed!"
                    self.counter.inc('CREATEFAILS')
                    return
        else:
            # job is already created
            job_ids.append(self.job_id)

        # do as many job gets as possible until thread is explictly stopped
        while not self.stopper.is_set():
            for job_id in job_ids:
                try:
                    self.client.get_job_info(job_id)
                    self.counter.inc('GET')
                except Exception:
                    self.counter.inc('GETFAILS')
        # cleanup this job
        for job_id in job_ids:
            self.client.stop_job(job_id)


if __name__ == "__main__":
    main()
