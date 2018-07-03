# Changelog for Peloton

0.8.0 (unreleased)
------------------

0.7.1.2
------------------
* 2018-07-03    Don't enforce instance config to use mesos containerizer for secrets                   adityacb@uber.com
* 2018-07-03    Base64 decode data before launching task                                               adityacb@uber.com

0.7.1.1
------------------
* 2018-06-20    Set Mesos Task Labels for JobID, InstanceID, TaskID                                    rcharles@uber.com

0.7.1
------------------
* 2018-06-13    Remove GetJobConfig from DB in TaskQuery and TaskList                                  chunyang.shen@uber.com

0.7.0
------------------
* 2018-06-13    Add secrets log formatter to redact secret data in logs                                adityacb@uber.com
* 2018-06-13    Fixing flaky test case for Entitlement Calculation                                     mabansal@uber.com
* 2018-06-13    Record state transition durations for RMTask                                           avyas@uber.com
* 2018-06-13    Refactoring GetHosts in HostManager as well as some code cleanup                       mabansal@uber.com
* 2018-06-13    Adding Reserver in placement engine for host reservation                               mabansal@uber.com
* 2018-06-12    Refactor task_test.go to use test suite                                                zhixin@uber.com
* 2018-06-12    Refine Cassandra Table Attributes                                                      varung@uber.com
* 2018-06-12    Increase placement engine worker threads                                               varung@uber.com
* 2018-06-12    Job Update/Get API now supports secrets                                                adityacb@uber.com
* 2018-06-11    Move v0 Peloton API to protobuf/peloton/api/v0                                         min@uber.com
* 2018-06-08    Update changeLog version to max version plus one when update job config                zhixin@uber.com
* 2018-06-08    Fix deadlock in task tracker                                                           avyas@uber.com
* 2018-06-07    Make job recovery failure a fatal error                                                apoorvaj@uber.com
* 2018-06-07    Add API to query job and task cache                                                    zhixin@uber.com
* 2018-06-06    Job runtime and config read gets data from cache when possible                         zhixin@uber.com
* 2018-06-06    Format code using `gofmt`                                                              avyas@uber.com
* 2018-06-06    Make errChan buffered eq to len of jobsByBatch to prevent gorountine leak on recoverJobsBatch failure varung@uber.com
* 2018-06-06    Fix populateSecrets bug when launching tasks from jobmgr                               adityacb@uber.com
* 2018-06-05    Partially created job set to INITIALIZED and enqueue to goalstate engine               zhixin@uber.com
* 2018-06-05    Kill the orphan Mesos task before regenerate a new MesosTaskID                         chunyang.shen@uber.com
* 2018-06-04    Add more logs for placement engine                                                     varung@uber.com
* 2018-06-04    Remove statusUpdaterRM                                                                 zhixin@uber.com
* 2018-06-04    Add/cleanup logs to track stuck tasks after failure to launch in job manager           apoorvaj@uber.com
* 2018-06-04    All job config and runtime update go through cache                                     zhixin@uber.com
* 2018-06-04    Fix async pool test & minor code refactoring                                           varung@uber.com
* 2018-05-31    Reconcile jobs and tasks in KILLING state as well                                      apoorvaj@uber.com
* 2018-05-30    Add stop feature for async pool jobs                                                   varung@uber.com
* 2018-05-30    Add host manager API support in pCluster for cli                                       varung@uber.com
* 2018-05-29    Fix GetJobConfig to use the correct configuration version                              apoorvaj@uber.com
* 2018-05-28    Add metrics to goal state to help debugging                                            apoorvaj@uber.com
* 2018-05-25    Making consistent function calls in entitlement calculator and reducing duplicate code in tests mabansal@uber.com

0.6.14
------------------
* 2018-05-24    Big int in Cassandra casts to int64 Cast big int in Cassandra to int64 upon read       zhixin@uber.com
* 2018-05-24    Temporary fix to fail write API calls in a non-leader job manager                      apoorvaj@uber.com
* 2018-05-24    Fix race in the deadline queue unit tests                                              apoorvaj@uber.com
* 2018-05-24    Add debug log for run time updater                                                     zhixin@uber.com
* 2018-05-24    Add EnqueueJobWithDefaultDelay helper function to goal state                           apoorvaj@uber.com
* 2018-05-24    Add pit1-prod01 cluster to integ tests config                                          rcharles@uber.com
* 2018-05-24    Add CLI to get outstanding offers                                                      varung@uber.com
* 2018-05-24    Peloton Secrets first cut                                                              adityacb@uber.com
* 2018-05-23    Peloton archiver code to query and delete jobs                                         adityacb@uber.com
* 2018-05-23    In task start, enqueue job with a batching delay                                       apoorvaj@uber.com
* 2018-05-23    Adding TestUtils for tests to remove code duplication as well as fixing comments       mabansal@uber.com
* 2018-05-22    Add tests for internal hostmanager API service                                         varung@uber.com
* 2018-05-22    Enable log rotation in thermos for peloton daemons                                     apoorvaj@uber.com
* 2018-05-22    Reduce number of EnqueueJob calls                                                      apoorvaj@uber.com
* 2018-05-22    Removing duplication of the code and more consistent logging                           mabansal@uber.com
* 2018-05-18    Use the version in job configuration protobuf to track job version                     apoorvaj@uber.com
* 2018-05-18    Add Internal API to expose all offers                                                  varung@uber.com
* 2018-05-17    Adding GetHosts api from hostmanager to returns the list of hosts which
                matches the resources and constraints passed by placement engine                       mabansal@uber.com
* 2018-05-17    Add job query by time range integ test                                                 adityacb@uber.com
* 2018-05-17    Stop dispatcher in engine and minor refactoring                                        varung@uber.com
* 2018-05-17    All task runtime read calls to DB should be sent to cache                              apoorvaj@uber.com
* 2018-05-16    Add action for invalid job and task state                                              zhixin@uber.com
* 2018-05-16    Implement write through cache for task runtime in job manager                          apoorvaj@uber.com
* 2018-05-15    bump peloton-client version                                                            varung@uber.com
* 2018-05-15    Add protobug to integration test requirements.txt                                      zhixin@uber.com
* 2018-05-15    Add integration test for non-preemptible jobs                                          avyas@uber.com
* 2018-05-15    Add STARTING task state to goal state                                                  zhixin@uber.com
* 2018-05-14    Upgrade mesos version in pcluster and mesos proto files to version 1.6.0-rc1           varung@uber.com
* 2018-05-14    Fix locking of RMTask when transitioning during scheduling                             avyas@uber.com
* 2018-05-14    Fix flaky unit test in TestBucketEventProcessor                                        zhixin@uber.com
* 2018-05-14    Add task fail reason in the metrics                                                    chunyang.shen@uber.com
* 2018-05-14    Fix debian package build script                                                        chunyang.shen@uber.com
* 2018-05-11    Offer Pool sentry logs, bug fixes, refactoring & unit tests.                           varung@uber.com
* 2018-05-11    Fix flaky unit test in observer                                                        zhixin@uber.com
* 2018-05-11    Event stream client functions normally after restarts                                  zhixin@uber.com
* 2018-05-10    Add revocable resource support                                                         varung@uber.com
* 2018-05-10    Add QoS controller to pCluster                                                         varung@uber.com
* 2018-05-10    Preempt ignores task with KILLED goal state                                            zhixin@uber.com


0.6.13
------------------
* 2018-05-09    Add filtering `name` and `host` for task query                                         chunyang.shen@uber.com
* 2018-05-08    Add KILLING state for task                                                             zhixin@uber.com
* 2018-05-08    Deleted tasks from the gangs needs to be removed from gang and enqueued again          mabansal@uber.com
* 2018-05-08    Add option to show progress for `job stop` cli action                                  avyas@uber.com
* 2018-05-07    Making placement backoff feature enable/disbale via config                             mabansal@uber.com
* 2018-05-04    Update `respool dump` cli to honor `-j/--json` flag                                    avyas@uber.com
* 2018-05-04    Add job config validatation rule and refactor                                          zhixin@uber.com
* 2018-05-04    Bug fixes at host summary, refactoring and unit tests.                                 varung@uber.com
* 2018-05-03    Adding Placement backoff support in Peloton                                            mabansal@uber.com
* 2018-05-03    Check for cached job and task to be non nil before using it                            apoorvaj@uber.com
* 2018-05-03    Upgrade mesos version in pcluster and mesos proto files to  version 1.5.0-rc2          adityacb@uber.com
* 2018-05-02    Update `GetPendingTasks` API to include `NonPreemptible` queue                         avyas@uber.com
* 2018-05-02    Always evaluate tasks during recovery except for onces recovered by job recovery       apoorvaj@uber.com
* 2018-05-02    Update version of ledership and libkv package                                          zhixin@uber.com
* 2018-05-02    Update CHANGELOG for 0.6.12.1                                                          apoorvaj@uber.com
* 2018-05-01    revert protoc-gen-go version                                                           varung@uber.com
* 2018-05-01    Add job delete integration tests                                                       adityacb@uber.com
* 2018-05-01    Revert "fix unit test broken by revert of 4533a25"                                     apoorvaj@uber.com
* 2018-05-01    Fix build issue caused by incompatible protoc-gen-go binary.                           varung@uber.com
* 2018-05-01    Add integration test for Start and Stop Task API                                       zhixin@uber.com
* 2018-04-30    Fix missing TTL error when orphan tasks are killed                                     apoorvaj@uber.com
* 2018-04-30    Add sorting for task query                                                             chunyang.shen@uber.com
* 2018-04-30    Fix flaky unit test                                                                    zhixin@uber.com
* 2018-04-27    Modify preemption queue type (from RMTask to PreemptionCandidate)                      sachins@uber.com
* 2018-04-27    Update browse sandbox API to return mesos master hostname and port                     varung@uber.com
* 2018-04-26    Add hostmgr recovery to restore the contents of maintenance queue                      sachins@uber.com
* 2018-04-26    Make candidate resign leadership if GainedLeadershipCallback fails                     zhixin@uber.com
* 2018-04-25    Add Configurable FrameworkInfo Capability                                              varung@uber.com
* 2018-04-24    Add HostService handler code                                                           sachins@uber.com
* 2018-04-24    Bump go version to 1.10 & fix jenkins build.                                           varung@uber.com
* 2018-04-23    Add Offer Type for feedback.                                                           varung@uber.com
* 2018-04-23    Add retry for reconciler and explicit reconciliation on hostmgr or mesos master re-election varung@uber.com
* 2018-04-23    Add Get Mesos Master Host and Port Endpoint to Hostmgr Internal Service.               varung@uber.com
* 2018-04-23    Added more executor tests                                                              pourchet@uber.com
* 2018-04-23    Fill in reason from ResoureManager for PENDING tasks                                   zhixin@uber.com


0.6.12
------------------
* 2018-04-19    Hide admission of non-preemptible jobs behind a flag                                                              Anant Vyas
* 2018-04-20    Checking mesos taskId before removing task from tracker                                                           Mayank Bansal
* 2018-04-19    Enable Aurora health check for Peloton                                                                            Tengfei Mu
* 2018-04-19    Update changelog for 0.6.12                                                                                       Tengfei Mu
* 2018-04-19    Fixing race condition between removing task from tracker and adding the same task with different mesos task id    Mayank Bansal
* 2018-04-19    Update health.leader when candidate is not leader                                                                 Zhixin Wen
* 2018-04-19    Add Host APIs                                                                                                     Sachin Sharma
* 2018-04-18    Add comment for channel 'finished' in resmgr/recovery.go                                                          Sachin Sharma
* 2018-04-18    fix unit test broken by revert of 4533a25                                                                         Zhixin Wen
* 2018-04-18    eventstream client send correct purgeOffset upon restart                                                          Zhixin Wen
* 2018-04-18    unset completion time when task is running                                                                        Zhixin Wen
* 2018-04-17    Revert "Revert "Add 100k task per job limit to master code""                                                      Aditya Bhave
* 2018-04-17    Retry Do not recover FAILED jobs till archiver is committed                                                       Tengfei Mu
* 2018-04-17    Revert "Rearchitect the job manager to use the cache and the goal state engine"                                   Tengfei Mu
* 2018-04-17    Revert "Do not recover FAILED jobs till archiver is committed."                                                   Tengfei Mu
* 2018-04-17    Revert "Add 100k task per job limit to master code"                                                               Tengfei Mu
* 2018-04-17    Revert "Fix completion time for jobs moving from PENDING to KILLED"                                               Tengfei Mu
* 2018-04-16    Fix completion time for jobs moving from PENDING to KILLED                                                        Aditya Bhave
* 2018-04-16    Add max_retry_attempts for test__create_job to pass smoketest                                                     Chunyang Shen
* 2018-04-12    Add 100k task per job limit to master code                                                                        Aditya Bhave
* 2018-04-13    enable host tags for metrics                                                                                      Zhixin Wen
* 2018-04-10    Bump up C* timeouts and add timers to recovery code                                                               Aditya Bhave
* 2018-04-12    Add Host Maintenance API                                                                                          Sachin Sharma
* 2018-04-11    Change GC and compaction for tables with large partitions                                                         Aditya Bhave
* 2018-04-10    Adding errorcodes in communication between resmgr and jobmgr for enqueuegangs                                     Mayank Bansal
* 2018-04-10    fix potential memory leak in priorityQueue                                                                        Zhixin Wen
* 2018-03-28    Make preemptor aware of non-preemptible tasks                                                                     Anant Vyas
* 2018-03-26    Admission control for non-preemptible gangs                                                                       Anant Vyas
* 2018-04-09    remove unused api.ResultSet to pass lint                                                                          Zhixin Wen
* 2018-04-09    Reconcile Staging Tasks                                                                                           Varun Gupta
* 2018-04-05    Add script to do performance comparison betwwen two versions                                                      Chunyang Shen
* 2018-04-04    Push to registry docker-registry02-sjc1:5055                                                                      Chunyang Shen
* 2018-04-04    Do not recover FAILED jobs till archiver is committed.                                                            Apoorva Jindal
* 2018-04-03    Fix docker build script and update ATG registry                                                                   Chunyang Shen
* 2018-03-22    Rearchitect the job manager to use the cache and the goal state engine                                            Apoorva Jindal
* 2018-04-02    Add a log when transient DB error occur on the hostmgr eventstream path                                           Apoorva Jindal
* 2018-03-29    Fix resmgr reason for state transition                                                                            Apoorva Jindal
* 2018-04-02    Update Glide installation in Makefile                                                                             Chunyang Shen
* 2018-03-26    Don't log UUID in sentry error                                                                                    Anant Vyas
* 2018-03-04    Add a common library to implement a goal state engine                                                             Apoorva Jindal
* 2018-03-23    Rename metric tag from `type` to `result` for success/fail                                                        Charles Raimbert
* 2018-03-22    Delete job_index entry as part of DeleteJob                                                                       Aditya Bhave
* 2018-03-20    Address remaining review comments on in-memory DB                                                                 Apoorva Jindal


0.6.11
------------------
* 2018-03-21    Pin down YARPC version in glide to avoid `uber fx`                                             Charles Raimbert
* 2018-03-21    Use patched docker/libkv for ZooKeeper Leader Election                                         Charles Raimbert
* 2018-03-21    Use long running job fixture for `test__stop_long_running_batch_job_immediately`               Anant Vyas
* 2018-03-20    Modify GetTasksForJobAndStates to accept []TaskState parameter instead of []string             Sachin Sharma
* 2018-03-15    Add integration test for Job Query API                                                         Aditya Bhave
* 2018-03-16    Do not update the state transition reason on dequeue from placement engine                     Apoorva Jindal
* 2018-03-15    Correct scheduled task accounting in case of launch errors for maxRunningInstance feature      Apoorva Jindal
* 2018-03-04    Add cache to job manager.                                                                      Apoorva Jindal
* 2018-03-19    Adding support for static respool in Tree hierarchy and Entitlement                            Mayank Bansal
* 2018-03-12    Add support to query jobs by timerange                                                         Aditya Bhave
* 2018-03-08    Be able to teardown vcluster in any fail in launching or testing vcluster                      Chunyang Shen
* 2018-03-14    Add runtime info to jobquery cli output                                                        Aditya Bhave
* 2018-03-14    Always evaluate a job for maxRunningInstaces SLA irrespective of job runtime updater result    Apoorva Jindal
* 2018-03-13    Adding Static reservation type in to resourcepool config                                       Mayank Bansal
* 2018-03-08    Add integration tests for controller task                                                      Anant Vyas
* 2018-03-06    Add a monitor job for vcluster to send data to M3                                              Chunyang Shen
* 2018-03-08    Enable integration test for fetching logs of previous task runs of failed task                 Apoorva Jindal
* 2018-03-09    Dividing entitlement calculation to phases and adding more tests to entitlement                Mayank Bansal
* 2018-03-07    Do not overwrite killed state for partially completed jobs                                     Apoorva Jindal
* 2018-03-08    Add 'task query' command to CLI to query on tasks(for a job) by state(s)                       Sachin Sharma
* 2018-03-07    Fix race condition in state machine rollback                                                   Anant Vyas

0.6.10.5
------------------
* 2018-03-07    Revert range map change                                                               mu@uber.com

0.6.10.4
------------------
* 2018-03-06    Remove 7 day time span restriction from querying active jobs                           adityacb@uber.com

0.6.10.3
------------------
* 2018-03-05    Handle incomplete killed jobs                                                          apoorvaj@uber.com
* 2018-03-01    Terminate the statemachine when a task is removed from the tracker                     avyas@uber.com

0.6.10.2
------------------
* 2018-03-02    Revert DequeueGang to get CONTROLLER task as well                                      avyas@uber.com

0.6.10.1
------------------
* 2018-02-28    Revert "Add 'task query' command to CLI to query on tasks(for a job) by state(s)"      rcharles@uber.com

0.6.10
------------------
* 2018-02-28    Bump peloton apps mem limit to 16GB                                                    avyas@uber.com
* 2018-02-28    Enable log rotation for Peloton containers                                             apoorvaj@uber.com
* 2018-02-28    Add 'task query' command to CLI to query on tasks(for a job) by state(s)               sachins@uber.com
* 2018-02-28    [API] Extend pending tasks API to work with controller queue                           avyas@uber.com
* 2018-02-28    Bump thrift version for peloton deployment tool                                        mu@uber.com
* 2018-02-27    JobQuery optimization                                                                  adityacb@uber.com
* 2018-02-27    Fix updating a nil controller limit metric                                             avyas@uber.com
* 2018-02-27    Change peloton deploy script to honor apps deployment order                            mu@uber.com
* 2018-02-27    Remove unused import                                                                   avyas@uber.com
* 2018-02-27    Fix periodic leader election metrics                                                   rcharles@uber.com
* 2018-02-27    [API] Add API and CLI to fetch active and pending tasks from resource manager          apoorvaj@uber.com
* 2018-02-27    Controller tasks scheduling and admission : part two                                   avyas@uber.com
* 2018-02-27    Expose placement reason to resmgr for state tracking                                   mu@uber.com
* 2018-02-27    Retry connection with ZooKeeper if connection dropped                                  rcharles@uber.com
* 2018-02-27    Fix pcluster for ZK stability                                                          rcharles@uber.com
* 2018-02-26    Persist FAILED task state into DB even if task needs to be restarted                   apoorvaj@uber.com
* 2018-02-25    Store reason for a state transition in the state machine                               apoorvaj@uber.com
* 2018-02-25    [API] Add controller limit in the resource pool config                                 avyas@uber.com
* 2018-02-24    API: Add summaryOnly flag to QueryRequest                                              adityacb@uber.com
* 2018-02-23    Refactor allocation accounting in resource manager                                     avyas@uber.com
* 2018-02-22    Change job runtime updater run interval for batch to be 10s and recover jobs in KILLING state. apoorvaj@uber.com
* 2018-02-22    Controller tasks scheduling and admission : part one                                   avyas@uber.com
* 2018-02-22    Add support for fetching task logs for previous task runs                              apoorvaj@uber.com
* 2018-02-22    Create respool based on the size of vcluster                                           chunyang.shen@uber.com
* 2018-02-22    Clean up old Aurora job files for Peloton deployment                                   min@uber.com
* 2018-02-22    Add a separate Makefile target to generate API docs                                    min@uber.com
* 2018-02-22    Emit Leader Election `is_leader` metrics every 10 secs                                 rcharles@uber.com
* 2018-02-22    [API] Add Controller task type in the peloton API                                      avyas@uber.com
* 2018-02-22    Fix JobSummary for old jobs, add job query by name                                     adityacb@uber.com
* 2018-02-21    Add maintenance mode to Peloton using Mesos maintenance primitives                     cjketchum@uber.com
* 2018-02-21    Fix ZooKeeper Leader Election metrics                                                  rcharles@uber.com
* 2018-02-21    Revert allocation metric name                                                          avyas@uber.com
* 2018-02-20    Add API to be able to fetch different task (runs) of an instance                       apoorvaj@uber.com
* 2018-02-20    Fix sorting in job query response                                                      adityacb@uber.com
* 2018-02-16    [API]: add job query by owner, wildcard search                                         adityacb@uber.com
* 2018-02-16    Merge branch master into release                                                       apoorvaj@uber.com
* 2018-02-16    Fix `allocation` metric scope name                                                     avyas@uber.com
* 2018-02-16    Add script for run multiple testing                                                    chunyang.shen@uber.com
* 2018-02-14    Update root resource pool `limit` with cluster capacity                                avyas@uber.com

0.6.9
------------------
* 2018-02-14    Log debug (not error) messages from job/task read handlers                             apoorvaj@uber.com
* 2018-02-13    Track allocation of non-preemptible tasks separately                                   avyas@uber.com
* 2018-02-13    Add archiver component to peloton                                                      adityacb@uber.com
* 2018-02-12    Batch tasks being sent to resource manager for maximum running instances feature.      apoorvaj@uber.com
* 2018-02-12    Fix demand calculation in entitlement calculator                                       avyas@uber.com
* 2018-02-12    Update release version in engdocs                                                      avyas@uber.com
* 2018-02-12    Add integration test for job update RPC                                                avyas@uber.com
* 2018-02-09    Evaluate and update job state even if the task stats have not changed.                 apoorvaj@uber.com
* 2018-02-09    Add refresh API for both job and task                                                  apoorvaj@uber.com
* 2018-02-09    Implemented core executor code                                                         pourchet@uber.com
* 2018-02-09    A task in launched state times out in job manager                                      apoorvaj@uber.com
* 2018-02-08    Handle kill for a job which has not fully created all tasks.                           apoorvaj@uber.com
* 2018-02-08    Send JobSummary in Job Query Response                                                  adityacb@uber.com
* 2018-02-08    Run job runtime updater as part of job goal state                                      apoorvaj@uber.com
* 2018-02-08    Handle initialized tasks with goal state set to be failed.                             apoorvaj@uber.com
* 2018-02-08    Add GetPendingTasks API in resource manager                                            avyas@uber.com
* 2018-02-07    Drop the old lucene index and the unused upgrades table in the next migration.         apoorvaj@uber.com
* 2018-02-07    Fix Down Sync migration script for update_info                                         adityacb@uber.com

0.6.8.2
------------------
* 2018-02-06    Untrack failed tasks with goal state succeeded.                                                  Apoorva Jindal

0.6.8.1
------------------
* 2018-02-03    Fix migrate script for job_index                                                                 Aditya Bhave

0.6.8
------------------
* 2018-02-02    Removing race between different transitions in state machine                                     Mayank Bansal
* 2018-02-02    Adding mesos quota support in cluster capacity call for host manager                             Mayank Bansal
* 2018-01-31    Schema and DB change to speed up JobQuery                                                        Aditya Bhave
* 2018-02-02    Adding Limit support for resource pools                                                          Mayank Bansal
* 2018-01-31    Adding apidoc in docs folder from build                                                          Mayank Bansal
* 2018-01-31    Adding peloton engdocs                                                                           Mayank Bansal
* 2018-01-02    Add extra logging in state machine implementation                                                Anant Vyas
* 2018-01-31    Changing api docs to html format                                                                 Mayank Bansal
* 2018-01-25    Ignore failure event due to duplicate task ID message from Mesos                                 Apoorva Jindal
* 2018-01-26    Send kill of PENDING tasks to resource manager                                                   Apoorva Jindal
* 2018-01-24    Send initialized tasks during recovery as a batch to resource manager                            Apoorva Jindal
* 2018-01-24    Guard against any case when hostname may be missing in offer pool.                               Zhitao Li
* 2018-01-22    Add Script for performance test running                                                          Chunyang Shen
* 2018-01-11    Fix sorting based on creation/completion time in job query                                       Apoorva Jindal
* 2018-01-24    Do not run job action with a context timeout.                                                    Apoorva Jindal
* 2018-01-23    Revert "Temporarily, do not recover initialized tasks in non-initialized jobs in job manager"    Apoorva Jindal
* 2018-01-08    shutdown executor after task kill timeout                                                        Chunyang Shen

0.6.7
------------------
* 2018-01-19    Do not recover KILLED jobs.                                                            apoorvaj@uber.com

0.6.6
------------------
* 2018-01-18    Change update task runtime success message to debug.                                   apoorvaj@uber.com
* 2018-01-18    PENDING tasks should not be re-sent to resource manager                                apoorvaj@uber.com
* 2018-01-18    Cleanup in placement processor                                                         apoorvaj@uber.com
* 2018-01-18    Do not update task runtime for all orphan tasks                                        mu@uber.com
* 2018-01-18    Add a stateful integration tests                                                       kejlberg@uber.com
* 2018-01-18    Bugfix Mimir placement strategy and bump Mimir-lib                                     kejlberg@uber.com
* 2018-01-16    Fix make test.                                                                         apoorvaj@uber.com
* 2018-01-12    Task runtime information in the cache should be either nil or in sync with DB          apoorvaj@uber.com
* 2018-01-12    Mesos state STAGING maps to LAUNCHED state in peloton.                                 apoorvaj@uber.com
* 2018-01-12    Adding Demand metrics as well updating static metrics with dynamic metrics             mabansal@uber.com
* 2018-01-11    Do not recover old terminated batch jobs with unknown goal state.                      apoorvaj@uber.com
* 2018-01-11    Automatically set GOMAXPROCS to match Linux container CPU quota                        avyas@uber.com
* 2018-01-10    Fix stateful placement engine to dequeue and place stateful tasks                      mu@uber.com
* 2018-01-09    Add a counter about number of hosts acquired and released on hostmgr                   zhitao@uber.com
* 2018-01-09    Add update API and DB schema                                                           apoorvaj@uber.com
* 2018-01-08    Add 50k & 100k tasks perf base jobs                                                    rcharles@uber.com
* 2018-01-08    Fix logging for ELK ingestion                                                          rcharles@uber.com
* 2018-01-08    Change the placement models so that they will be json serialized when using them in logging fields. kejlberg@uber.com
* 2018-01-05    Adding API doc in peloton repo                                                         mabansal@uber.com
* 2018-01-04    Add ability to preempt PLACING tasks                                                   avyas@uber.com
* 2018-01-04    Use mimir placement strategy for stateful task placement                               mu@uber.com
* 2018-01-04    The placement engine now returns failed tasks to the resource manager.                 kejlberg@uber.com
* 2018-01-04    Add support for re-enqueuing unplaced tasks into the resource manager                  kejlberg@uber.com

0.6.5
------------------
* 2018-01-03    Skip reschedule stateful task upon task lost event                                     mu@uber.com
* 2018-01-03    Refactor and add tests to resource manager `respool` pkg                               avyas@uber.com
* 2018-01-03    Implemented API to get Task Events                                                     adityacb@uber.com
* 2018-01-02    Make kill job faster and fix regression in create job                                  apoorvaj@uber.com
* 2018-01-02    Do not reschedule already scheduled INITIALIZED tasks                                  apoorvaj@uber.com
* 2017-12-29    Virtual Mesos cluster setup through Peloton Client                                     chunyang.shen@uber.com
* 2017-12-29    Add cli command to list and clean persistent volume                                    mu@uber.com
* 2017-12-29    Update volume state to be DELETED in resource cleaner                                  mu@uber.com
* 2017-12-29    Eable "shutdown executor" for hotmgr                                                   chunyang.shen@uber.com
* 2017-12-28    Implement job stop using job goal state                                                apoorvaj@uber.com
* 2017-12-28    Take lock before reading/writing to job struct                                         apoorvaj@uber.com
* 2017-12-27    Acquire read lock before getting job in tracked manager                                apoorvaj@uber.com
* 2017-12-27    Fix deployment script to ignore apps that doesn't exist                                mu@uber.com
* 2017-12-26    Added mesos client for executor                                                        pourchet@uber.com
* 2017-12-26    Add option to start stateful placement engine in deployment script                     mu@uber.com
* 2017-12-22    Allow a job configuration without a default configuration.                             apoorvaj@uber.com
* 2017-12-22    Add support for MaximumRunningInstances SLA configuration.                             apoorvaj@uber.com
* 2017-12-21    Implement job recovery in goal state engine in job manager                             apoorvaj@uber.com
* 2017-12-20    Move task state to PENDING after enqueuing it to resource manager.                     apoorvaj@uber.com
* 2017-12-20    [hostmgr] Separate reporting between no offer and mismatch status.                     zhitao@uber.com
* 2017-12-15    Move creation of tasks and recovery into job goal state                                apoorvaj@uber.com
* 2017-12-15    Change scalar.Resources methods from pointer receiver to non-pointer.                  zhitao@uber.com


0.6.4
------------------
- 2017-12-14    Skip terminal jobs during job manager sync from DB                                     @apoorvaj
- 2017-12-14    Added the mesos podtask                                                                @pourchet


0.6.3
------------------
- 2017-12-14    Increase MaxRecvMsgSize in gRPC to 256MB                                               @min
- 2017-12-14    Merge the placement engine from the master branch into release                         @kejlberg
- 2017-12-13    Move metrics gauage update to asynchronous                                             @zhitao
- 2017-12-13    Update volume state upon stateful task running status update                           @mu
- 2017-12-13    Add more logging for jobmgr to launch stateful                                         @mu
- 2017-12-13    Fixing Integration test preprod cluster zk address                                     @mabansal
- 2017-12-13    Add reservation cleaner to clean both unused volume and resources                      @mu
- 2017-12-13    Add job goal state to job manager                                                      @apoorvaj
- 2017-12-13    Add materialized view for volume by state                                              @mu

0.6.2
------------------
- 2017-12-12    Adding more logging to entitlelement calculator in resmgr                              @Mayank Bansal
- 2017-12-12    Revert "Check in mocks"        							                               @Antoine Pourchet
- 2017-12-12    Adding deadline feature in Peloton                                                     @Mayank Bansal
- 2017-12-08    Add changelog for changes between 0.5.0 and 0.6.0                                      @Anant Vyas

0.6.1
------------------
- 2017-12-08    Improve Resource Manager recovery performance                                          @Anant Vyas
- 2017-12-06    Add materialized view for volumes by job ids                                           @Tengfei Mu
- 2017-12-07    Update task runtime state when receiving a mesos kill event                            @Apoorva Jindal
- 2017-12-06    Do not update runtime reason on mesos update always                                    @Apoorva Jindal
- 2017-12-06    Move volumesvc from hostmgr to jobmgr                                                  @Tengfei Mu
- 2017-12-07    Check in mocks                                                                         @Tengfei Mu
- 2017-12-05    Kill orphaned tasks in mesos                                                           @Apoorva Jindal
- 2017-12-05    Implement volume list and delete API                                                   @Tengfei Mu
- 2017-12-04    Add reason and message for every update to task runtime                                @Apoorva Jindal
- 2017-12-04    Return failed instance list in task stop and task start                                @Apoorva Jindal
- 2017-12-01    Handle task start of failed tasks                                                      @Apoorva Jindal
- 2017-11-28    Restart the goal state when placement received for a task which needs to be killed.    @Apoorva Jindal
- 2017-11-29    Handle stopped tasks during reconcialiation.                                           @Apoorva Jindal
- 2017-12-01    Add yaml files for performance tests                                                   @Apoorva Jindal
- 2017-11-30    Remove smoketest tag from preemption integ test                                        @Anant Vyas
- 2017-11-21    Porting storage changes from master to release                                         @Apoorva Jindal

