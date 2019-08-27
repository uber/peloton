#!/usr/bin/env python
"""
This is a script for launch different benchmark jobs with:
    Instance number: 10k, 50k
    Sleep time: 10s, 300s,
    Instance config: true, false
on the cluster configured in the file '.vcluster'. The file
'.vcluster' contains keys 'Zookeeper', 'Peloton Version' and
'Mesos Slave Number'.

"""
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from collections import Counter

import json
import os
import pandas as pd
import numpy as np
import sys
import threading
import time

from performance_test_client import (
    PerformanceTestClient,
    ResPoolNotFoundException,
)

from aurora_bridge_client import api
from aurora_bridge_util import (
    assert_keys_equal,
    get_job_update_request,
    get_update_status,
    start_job_update,
    wait_for_rolled_forward,
    wait_for_rolled_back,
    wait_for_update_status,
    verify_host_limit_1,
)

# Perf tests conducted. Serves as a single source of truth.
# Within each tuple, first item explains the perf test purpose;
# second item shows the corresponding csv file structure.
PERF_TEST_CONDUCTED = [
    ('JOB_CREATE', '_job_create.csv'),
    ('JOB_GET', '_job_get.csv'),
    ('JOB_UPDATE', '_job_update.csv'),
    ('JOB_STATELESS_CREATE', '_job_stateless_create.csv'),
    ('JOB_STATELESS_UPDATE', '_job_stateless_update.csv'),
    ('JOB_PARALLEL_STATELESS_UPDATE', '_job_parallel_stateless_update.csv'),
    ('JOB_STATELESS_HOST_LIMIT_1_CREATE', '_job_stateless_host_limit_1_create.csv'),
    ('JOB_STATELESS_HOST_LIMIT_1_UPDATE', '_job_stateless_host_limit_1_update.csv'),
]

NUM_TASKS = [50000]
SLEEP_TIME_SEC = [10]
USE_INSTANCE_CONFIG = [True, False]

stats_keys = ["CREATE", "CREATEFAILS", "GET", "GETFAILS"]


def parse_arguments(args):

    parser = ArgumentParser(
        description="", formatter_class=RawDescriptionHelpFormatter
    )

    # Input the vCluster config file
    parser.add_argument(
        "-i",
        "--input-file",
        dest="input_file",
        help="the input config file of vCluster",
    )

    # Output the performance data
    parser.add_argument(
        "-o",
        "--output-file-prefix",
        dest="output_file_prefix",
        help="the output file prefix to store a list finished perf results",
    )

    # Run a mini test suite
    parser.add_argument(
        "--mini",
        action="store_true",
        default=False,
        help="run a mini test suite",
    )

    return parser.parse_args(args)


def output_files_list(res_dir, base_output):
    """
    Generates a set of desired output file names.

    Args:
        res_dir: absolute path of the result output files.
        base_output: user specified output file as basis of generated file set.

    Returns:
        a list of csv files where perf test results will be written to.
    """
    suffix = [t[1] for t in PERF_TEST_CONDUCTED]
    output_file_list = [os.path.join(res_dir, base_output + s) for s in suffix]
    return output_file_list


def main():
    args = parse_arguments(sys.argv[1:])
    vcluster_config = args.input_file
    res_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "PERF_RES"
    )
    if not os.path.exists(res_dir):
        os.makedirs(res_dir)
    base_csv_name = args.output_file_prefix
    output_csv_files_list = output_files_list(res_dir, base_csv_name)

    try:
        cluster_config = json.loads(open(vcluster_config).read())
        agent_num = int(cluster_config["Mesos Slave Number"])
        zkserver = cluster_config["Zookeeper"]
        peloton_version = cluster_config["Peloton Version"]
        pf_client = PerformanceTestClient(zkserver, agent_num, peloton_version)
    except IOError:
        print("Can't find the vcluster config file.")
        return
    except ValueError:
        print("This vcluster config is not a valid JSON file.")
        return
    except ResPoolNotFoundException:
        print("Can't find Respool for this Peloton Cluster.")
        return
    peloton_version = cluster_config["Peloton Version"]
    records = []

    if args.mini:
        print("Running mini test suite")
        # Run one test to spin 50k tasks, no instance config and 10 sec sleep
        record = run_one_test(pf_client, 50000, False, 10, agent_num)
        records.append(record)
    else:
        for num_tasks in NUM_TASKS:
            for sleep_time in SLEEP_TIME_SEC:
                for instance_config in USE_INSTANCE_CONFIG:
                    try:
                        record = run_one_test(
                            pf_client,
                            num_tasks,
                            instance_config,
                            sleep_time,
                            agent_num,
                        )
                    except Exception as e:
                        msg = (
                            "TaskNum %s && " % (num_tasks)
                            + "SleepTime %s && " % (sleep_time)
                            + "InstanceConfig %s" % (instance_config)
                        )
                        print(
                            "test create job: job creation failed: %s (%s)"
                            % (e, msg)
                        )
                        continue
                    records.append(record)

    df = pd.DataFrame(
        records,
        columns=[
            "Cores",
            "TaskNum",
            "Sleep(s)",
            "UseInsConf",
            "Start(s)",
            "Exec(s)",
        ],
    )
    print("Test Create")
    print(df)

    # write results to '$base_path$_job_create.csv'.
    df.to_csv(output_csv_files_list[0], sep="\t")

    if args.mini:
        return

    t = PerformanceTest(pf_client, peloton_version)

    # create 5000 jobs in 100 threads and count num of job.Get() that
    # can be done in 300sec
    t.perf_test_job_create_get(
        num_threads=100, jobs_per_thread=50, num_tasks=10
    )

    # create 1 job with 50k tasks and count num of job.Get() that can be done
    # by 100 threads in 300sec
    t.perf_test_job_create_get(num_threads=100, num_tasks=50000, get_only=True)

    # create 1 job with 50k instance configs and count num of job.Get() that
    # can be done by 1 thread in 300sec
    # use 1 thread because using multiple threads causes OOM on vcluster jobmgr
    t.perf_test_job_create_get(
        num_threads=1, num_tasks=50000, use_instance_config=True, get_only=True
    )

    # write test_job_create_get results to '$base_path$_job_get.csv'.
    get_df = t.dump_records_to_file()

    if get_df is not None:
        get_df.to_csv(output_csv_files_list[1], sep="\t")

    # write test_job_update results to '$base_path$_job_update.csv'.
    update_df = t.perf_test_job_update(
        num_start_tasks=1,
        tasks_inc=10,
        num_increment=500,
        sleep_time=30,
        use_instance_config=True,
    )

    if update_df is not None:
        update_df.to_csv(output_csv_files_list[2], sep="\t")

    # create one large stateless job (uses 90% of capacity)
    create_df = t.perf_test_stateless_job_create()
    if create_df is not None:
        create_df.to_csv(output_csv_files_list[3], sep='\t')

    # update one large stateless job (uses 90% of capacity)
    update_df = t.perf_test_stateless_job_update()
    if update_df is not None:
        update_df.to_csv(output_csv_files_list[4], sep='\t')

    # update multiple smaller stateless jobs in parallel
    # (total use 90% of capacity)
    pupdate_df = t.perf_test_stateless_parallel_updates()
    if pupdate_df is not None:
        pupdate_df.to_csv(output_csv_files_list[5], sep='\t')

    # create a stateless job with host-limit-1 constraint (use 60% of the cluster)
    host_limit_1_create_df = t.perf_test_job_stateless_host_limit_1_create()
    if host_limit_1_create_df is not None:
        host_limit_1_create_df.to_csv(output_csv_files_list[6], sep='\t')

    # update a stateless job with host-limit-1 constraint (use 60% of the cluster)
    host_limit_1_update_df = t.perf_test_stateless_job_host_limit_1_update()
    if host_limit_1_update_df is not None:
        host_limit_1_update_df.to_csv(output_csv_files_list[7], sep='\t')

    # Test AuroraBridge write path
    t.perf_aurora_bridge_test_write_path()


def run_one_test(pf_client, num_tasks, instance_config, sleep_time, agent_num):
    job = pf_client.get_batch_job()
    pf_client.create_job(job, num_tasks, instance_config, sleep_time)
    succeeded, start, completion = pf_client.monitoring_job(job)
    record = {
        "TaskNum": num_tasks,
        "Sleep(s)": sleep_time,
        "UseInsConf": instance_config,
        "Cores": agent_num,
    }
    if succeeded:
        record.update({"Start(s)": start, "Exec(s)": completion})
    print(record)
    return record


class perfCounter:
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


class completionCounter:
    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    def inc(self, value):
        with self.lock:
            self.counter += value

    def get(self):
        with self.lock:
            return self.counter


class PerformanceTest:
    def __init__(self, client, peloton_version):
        self.client = client
        self.peloton_version = peloton_version
        self.records = []

    def perf_test_job_update(
        self,
        num_start_tasks=1,
        tasks_inc=1,
        num_increment=300,
        sleep_time=30,
        use_instance_config=False,
    ):
        """
        perf_test_job_update can be used for testing create job and then
        update the job to increase instances.
        The test starts by creating one job with num_start_tasks(this task
        sleeps 120 seconds), and then
        loop num_increment times to add number of
        tasks_inc each time, each of the newly added tasks is sleeping for 30
        seconds,
        and calculate
        the total time it used to create and finish the job.
        """
        total_time_in_seconds = 0
        try:
            job = self.client.get_batch_job()
            self.client.create_job(
                job, num_start_tasks, use_instance_config, 120
            )
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s && InstanceConfig %s" % (
                num_start_tasks,
                sleep_time,
                use_instance_config,
            )
            print("test_job_update: create job failed: %s (%s)" % (e, msg))
            return
        for counter in xrange(num_increment):
            try:
                self.client.update_job(
                    job, tasks_inc, 0, use_instance_config, sleep_time
                )
            except Exception as e:
                msg = "NumStartTasks %s && TasksInc %s && NumIncrement %s" % (
                    num_start_tasks,
                    tasks_inc,
                    num_increment,
                )
                print("test_job_update: update job failed: %s (%s)" % (e, msg))
                return

        succeed, start, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        record = [
            {
                "NumStartTasks": num_start_tasks,
                "TaskIncrementEachTime": tasks_inc,
                "NumOfIncrement": num_increment,
                "Sleep(s)": sleep_time,
                "UseInsConf": use_instance_config,
                "TotalTimeInSeconds": total_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record,
            columns=[
                "NumStartTasks",
                "TaskIncrementEachTime",
                "NumOfIncrement",
                "Sleep(s)",
                "UseInsConf",
                "TotalTimeInSeconds",
            ],
        )
        print("Test Update")
        print(df)
        return df

    def perf_test_stateless_job_create(
        self,
        num_tasks=9000,
        sleep_time=1000,
    ):
        """
        perf_test_stateless_job_create is used to test creating a stateless
        job. The test creates one job with num_tasks and measures the time
        it takes for all tasks to become RUNNING. It should be run to
        consume no more than 90% of the respool capacity.
        """
        total_time_in_seconds = 0
        try:
            job = self.client.get_stateless_job()
            self.client.create_job(job, num_tasks, False, sleep_time)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_create: create job failed: %s (%s)"
                % (e, msg)
            )
            return

        succeed, _, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        try:
            self.client.stop_job(job, wait_for_kill=True)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_create: stop job failed: %s (%s)"
                % (e, msg)
            )
            return

        record = [
            {
                "NumTasks": num_tasks,
                "Sleep(s)": sleep_time,
                "TotalTimeInSeconds": total_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record, columns=["NumTasks", "Sleep(s)", "TotalTimeInSeconds"],
            dtype=np.int64
        )
        print("Test StatelessCreate")
        print(df)
        return df

    def perf_test_stateless_job_update(
        self, num_tasks=9000, batch_size=9000, sleep_time=1000
    ):
        """
        perf_test_stateless_job_update is used to test updating a stateless
        job. The test creates one job with num_tasks and measures the time
        it takes to update all tasks to a new version. It should be run to
        consume no more than 90% of the respool capacity.
        """
        total_time_in_seconds = 0
        try:
            job = self.client.get_stateless_job()
            self.client.create_job(job, num_tasks, False, sleep_time - 100)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_update: create job failed: "
                "%s (%s)" % (e, msg)
            )
            return

        succeed, _, _ = self.client.monitoring_job(job)
        if succeed is False:
            print("test_job_stateless_update: job failed to start")
            return

        try:
            self.client.update_job(job, 0, batch_size, False, sleep_time)
        except Exception as e:
            msg = "Num_tasks %s && SleepTime %s && BatchSize %s" % (
                num_tasks,
                sleep_time,
                batch_size,
            )
            print(
                "test_job_stateless_update: update job failed: %s (%s)"
                % (e, msg)
            )
            return

        succeed, _, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        try:
            self.client.stop_job(job, wait_for_kill=True)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_create: stop job failed: "
                "%s (%s)" % (e, msg)
            )
            return

        record = [
            {
                "NumTasks": num_tasks,
                "Sleep(s)": sleep_time,
                "BatchSize": batch_size,
                "TotalTimeInSeconds": total_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record,
            columns=[
                "NumTasks",
                "Sleep(s)",
                "BatchSize",
                "TotalTimeInSeconds",
            ],
        )
        print("Test StatelessUpdate")
        print(df)
        return df

    def perf_test_stateless_parallel_updates(
        self, num_jobs=100, num_tasks=90, batch_size=10, sleep_time=1000
    ):
        """
        perf_test_stateless_parallel_updates is used to test updating
        num_jobs number of stateless job in parallel. Each job has num_tasks,
        and the test measures the average completion time for updating
        each job using batch_size as the update batch size. It should be run
        to consume no more than 90% of the respool capacity.
        """
        counter = completionCounter()
        failure_counter = completionCounter()
        threads = []

        # Create a thread to create and update one job
        for i in range(num_jobs):
            t = statelessUpdateWorker(
                i,
                self.client,
                counter,
                failure_counter,
                num_tasks=num_tasks,
                batch_size=batch_size,
                sleep_time=sleep_time,
            )
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        failure_count = failure_counter.get()
        if failure_count > 0:
            msg = "statelessUpdateWorkers failed %s" % (failure_count)
            print(
                "perf_test_stateless_parallel_updates: update parallel "
                "jobs failed: (%s)" % (msg)
            )
            return

        average_time_in_seconds = counter.get() / num_jobs
        record = [
            {
                "NumJobs": num_jobs,
                "NumTasks": num_tasks,
                "Sleep(s)": sleep_time,
                "BatchSize": batch_size,
                "AverageTimeInSeconds": average_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record,
            columns=[
                "NumJobs",
                "NumTasks",
                "Sleep(s)",
                "BatchSize",
                "AverageTimeInSeconds",
            ],
            dtype=np.int64
        )
        print("Test ParallelStatelessUpdate")
        print(df)
        return df

    def perf_test_job_create_get(
        self,
        num_threads=10,
        jobs_per_thread=5,
        num_tasks=10,
        sleep_time=10,
        use_instance_config=False,
        get_only=False,
    ):
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
                job = self.client.get_batch_job()
                self.client.create_job(
                    job, num_tasks, use_instance_config, sleep_time
                )
                self.counter.inc("CREATE")
            except Exception as e:
                msg = "TaskNum %s && SleepTime %s && InstanceConfig %s" % (
                    num_tasks,
                    sleep_time,
                    use_instance_config,
                )
                print(
                    "test_job_create_get: create job failed: %s (%s)"
                    % (e, msg)
                )
                self.counter.inc("CREATEFAILS")
                return
        else:
            job = None

        for i in xrange(num_threads):
            t = jobWorker(
                self.client,
                num_tasks,
                jobs_per_thread,
                stopper,
                use_instance_config,
                job,
                sleep_time,
                self.counter,
            )
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
            "TaskNum": num_tasks,
            "Sleep(s)": sleep_time,
            "UseInsConf": use_instance_config,
            "Creates": counts["CREATE"],
            "CreateFails": counts["CREATEFAILS"],
            "Gets": counts["GET"],
            "GetFails": counts["GETFAILS"],
        }
        self.records.append(record)

    def dump_records_to_file(self):
        df = pd.DataFrame(
            self.records,
            columns=[
                "TaskNum",
                "Sleep(s)",
                "UseInsConf",
                "Creates",
                "CreateFails",
                "Gets",
                "GetFails",
            ],
        )
        print("Test Create + Get")
        print(df)
        return df

    def perf_test_job_stateless_host_limit_1_create(
            self,
            num_tasks=600,
            sleep_time=1000,
    ):
        """
        perf_test_job_stateless_host_limit_1_create is used to test creating a
        stateless job with host-limit-1 constraint. The test creates one job
        with num_tasks and measures the time it takes for all tasks to become
        RUNNING.
        """
        total_time_in_seconds = 0
        try:
            job = self.client.get_stateless_job()
            self.client.create_job(
                job,
                num_tasks,
                False,
                sleep_time,
                host_limit_1=True,
            )
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless__host_limit_1_create: create job failed: %s (%s)"
                % (e, msg)
            )
            return

        succeed, _, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        try:
            self.client.stop_job(job, wait_for_kill=True)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_host_limit_1_create: stop job failed: %s (%s)"
                % (e, msg)
            )
            return

        record = [
            {
                "NumTasks": num_tasks,
                "Sleep(s)": sleep_time,
                "TotalTimeInSeconds": total_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record, columns=["NumTasks", "Sleep(s)", "TotalTimeInSeconds"],
            dtype=np.int64
        )
        print("Test StatelessHostLimit1Create")
        print(df)
        return df

    def perf_test_stateless_job_host_limit_1_update(
            self,
            num_tasks=600,
            batch_size=20,
            sleep_time=1000,
    ):
        """
        perf_test_stateless_job_host_limit_1_update is used to test updating a
        stateless job with host_limit_1 constraint. The test creates one job
        with num_tasks and measures the time it takes to update all tasks to a
        new version.
        """
        total_time_in_seconds = 0
        try:
            job = self.client.get_stateless_job()
            self.client.create_job(
                job,
                num_tasks,
                False,
                sleep_time - 100,
                host_limit_1=True,
            )
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_host_limit_1_update: create job failed: "
                "%s (%s)" % (e, msg)
            )
            return

        succeed, _, _ = self.client.monitoring_job(job)
        if succeed is False:
            print("test_job_stateless_host_limit_1_update: job failed to start")
            return

        try:
            self.client.update_job(
                job, 0, batch_size, False, sleep_time, host_limit_1=True)
        except Exception as e:
            msg = "Num_tasks %s && SleepTime %s && BatchSize %s" % (
                num_tasks,
                sleep_time,
                batch_size,
            )
            print(
                "test_job_stateless_host_limit_1_update: update job failed: %s (%s)"
                % (e, msg)
            )
            return

        succeed, _, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        try:
            self.client.stop_job(job, wait_for_kill=True)
        except Exception as e:
            msg = "Num_start_tasks %s && SleepTime %s" % (
                num_tasks,
                sleep_time,
            )
            print(
                "test_job_stateless_host_limit_1_create: stop job failed: "
                "%s (%s)" % (e, msg)
            )
            return

        record = [
            {
                "NumTasks": num_tasks,
                "Sleep(s)": sleep_time,
                "BatchSize": batch_size,
                "TotalTimeInSeconds": total_time_in_seconds,
            }
        ]
        df = pd.DataFrame(
            record,
            columns=[
                "NumTasks",
                "Sleep(s)",
                "BatchSize",
                "TotalTimeInSeconds",
            ],
        )
        print("Test StatelessHostLimit1Update")
        print(df)
        return df

    def perf_aurora_bridge_test_write_path(self):
        """
        - Create a stateless job via AuroraBridge, #tasks = 600 and batch_size = 50
        - Create a healthy update for stateless job, with #tasks = 600 and batch_size = 50
        - Create a unhealthy update with auto-rollback at 200 failed instances.
        - Create pinned instances, where C1: 0-100, C2: 101-300, C3: 301-599
        - Deploy C4 for all tasks, and trigger manual rollback.
        - Once manual rollback completes, validate the correctness and calculate the time taken.
        """
        try:
            start_time = time.time()

            print 'Create Stateless Job of 600 instances and batch size 50'
            step_start_time = time.time()
            start_job_update(
                self.client.aurora_bridge_client,
                get_job_update_request("test_dc_labrat.yaml"),
                "create job",
            )
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print '\nUpdate Stateless Job of 600 instances with healthy config and batch size 50'
            step_start_time = time.time()
            start_job_update(
                self.client.aurora_bridge_client,
                get_job_update_request("test_dc_labrat_update.yaml"),
                "start job update",
            )
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print '\nRollout bad config which will trigger auto-rollback after 200 failed instances'
            step_start_time = time.time()
            resp = self.client.aurora_bridge_client.start_job_update(
                get_job_update_request("test_dc_labrat_bad_config.yaml"),
                "start job update bad config",
            )
            job_update_key = resp.key
            wait_for_rolled_back(
                self.client.aurora_bridge_client, job_update_key)
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print '\nRollout Pinned Instance Config (C1) for Instance: 0-99'
            step_start_time = time.time()
            instances = []
            for i in xrange(100):
                instances.append(i)
            pinned_req = get_job_update_request(
                "test_dc_labrat.yaml"
            )
            pinned_req.settings.updateOnlyTheseInstances = set(
                [api.Range(first=i, last=i) for i in instances]
            )
            job_key = start_job_update(
                self.client.aurora_bridge_client,
                pinned_req,
                "update pinned instance req",
            )
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print '\nRollout Pinned Instance Config (C3) for Instance: 301-599'
            step_start_time = time.time()
            instances = []
            for i in xrange(301, 600):
                instances.append(i)
            pinned_req = get_job_update_request(
                "test_dc_labrat_update2.yaml"
            )
            pinned_req.settings.updateOnlyTheseInstances = set(
                [api.Range(first=i, last=i) for i in instances]
            )
            start_job_update(
                self.client.aurora_bridge_client,
                pinned_req,
                "update pinned instance req",
            )
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print '\nRollout update and trigger manual rollback'
            step_start_time = time.time()
            resp = self.client.aurora_bridge_client.start_job_update(
                get_job_update_request("test_dc_labrat.yaml"),
                "start job update with good config",
            )
            job_update_key = resp.key
            time.sleep(100)
            self.client.aurora_bridge_client.rollback_job_update(job_update_key)
            wait_for_rolled_back(
                self.client.aurora_bridge_client, job_update_key)
            print('Time Taken:: %f' % (time.time() - step_start_time))

            print 'Validate pinned instance configs are set correctly'
            step_start_time = time.time()
            resp = self.client.aurora_bridge_client.get_tasks_without_configs(
                api.TaskQuery(jobKeys={job_key}, statuses={
                              api.ScheduleStatus.RUNNING})
            )
            assert len(resp.tasks) == 600

            print('Time Taken:: %f' % (time.time() - step_start_time))

            elapsed_time = time.time() - start_time
            print('\n\nTotal Time Taken:: %f' % (elapsed_time))

            for t in resp.tasks:
                _, instance_id, _ = t.assignedTask.taskId.rsplit("-", 2)
                if int(instance_id) < 100:
                    for m in t.assignedTask.task.metadata:
                        if m.key == "test_key_1":
                            assert m.value == "test_value_1"
                        elif m.key == "test_key_2":
                            assert m.value == "test_value_2"
                elif int(instance_id) >= 100 and int(instance_id) < 300:
                    if m.key == "test_key_11":
                        assert m.value == "test_value_11"
                    elif m.key == "test_key_22":
                        assert m.value == "test_value_22"
                else:
                    if m.key == "test_key_111":
                        assert m.value == "test_value_111"
                    elif m.key == "test_key_222":
                        assert m.value == "test_value_222"

        except Exception as e:
            print e


class statelessUpdateWorker(threading.Thread):
    """
    statelessUpdateWorker is a worker class which creates a
    new job and then updates it to a new version
    """

    def __init__(
        self,
        index,
        client,
        counter,
        failure_counter,
        num_tasks=10000,
        batch_size=10000,
        sleep_time=1000,
    ):
        threading.Thread.__init__(self)
        self.index = index
        self.client = client
        self.counter = counter
        self.failure_counter = failure_counter
        self.num_tasks = num_tasks
        self.batch_size = batch_size
        self.sleep_time = sleep_time

    def run(self):
        total_time_in_seconds = 0
        try:
            job = self.client.get_stateless_job()
            self.client.create_job(
                job, self.num_tasks, False, self.sleep_time - 100
            )
        except Exception as e:
            msg = "index %s" % (self.index)
            print(
                "statelessUpdateWorker: create job failed: %s (%s)" % (e, msg)
            )
            self.failure_counter.inc(1)
            return

        succeed, _, _ = self.client.monitoring_job(job)
        if succeed is False:
            msg = "index %s" % (self.index)
            print("statelessUpdateWorker: job failed to start: %s" % (msg))
            self.failure_counter.inc(1)
            return

        try:
            self.client.update_job(
                job, 0, self.batch_size, False, self.sleep_time
            )
        except Exception as e:
            msg = "index %s && BatchSize %s" % (self.index, self.batch_size)
            print(
                "statelessUpdateWorker: update job failed: %s (%s)" % (e, msg)
            )
            self.failure_counter.inc(1)
            return

        succeed, _, completion = self.client.monitoring_job(job)
        if succeed:
            total_time_in_seconds = completion

        try:
            self.client.stop_job(job, wait_for_kill=True)
        except Exception as e:
            msg = "index %s" % (self.index)
            print("statelessUpdateWorker: stop job failed: %s (%s)" % (e, msg))
            self.failure_counter.inc(1)
            return

        self.counter.inc(total_time_in_seconds)
        return


class jobWorker(threading.Thread):
    """
    jobWorker is a worker class that can be used to perform job.Create() and
    job.Get() at scale
    """

    def __init__(
        self,
        client,
        num_tasks,
        jobs_per_thread,
        stopper,
        use_instance_config=False,
        job=None,
        sleep_time=10,
        counter=None,
    ):
        threading.Thread.__init__(self)
        self.client = client
        self.num_tasks = num_tasks
        self.jobs_per_thread = jobs_per_thread
        self.stopper = stopper
        self.use_instance_config = use_instance_config
        self.job = job
        self.sleep_time = sleep_time
        self.counter = counter

    def run(self):
        jobs = []
        if self.job is None:
            # create job then run job.Get()
            for i in xrange(self.jobs_per_thread):
                try:
                    job = self.client.get_batch_job()
                    self.client.create_job(
                        job,
                        self.num_tasks,
                        self.use_instance_config,
                        self.sleep_time,
                    )
                    self.counter.inc("CREATE")
                    jobs.append(job)
                except Exception as e:
                    msg = "TaskNum=%s && SleepTime %s && InstanceConfig %s" % (
                        self.num_tasks,
                        self.sleep_time,
                        self.use_instance_config,
                    )
                    print("jobWorker: create job failed: %s (%s)" % (e, msg))
                    self.counter.inc("CREATEFAILS")
                    return
        else:
            # job is already created
            jobs.append(self.job)

        # do as many job gets as possible until thread is explictly stopped
        while not self.stopper.is_set():
            for job in jobs:
                try:
                    self.client.get_job_info(job)
                    self.counter.inc("GET")
                except Exception:
                    self.counter.inc("GETFAILS")
        # cleanup this job
        for job in jobs:
            self.client.stop_job(job)


if __name__ == "__main__":
    main()
