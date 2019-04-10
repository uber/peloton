# Changelog for Peloton
0.8.4 (unreleased)
------------------

0.8.3.1
------------------
* 2019-04-10    Add PodConfigurationStateStats in job status                                           zhixin@uber.com.

0.8.3
------------------
* 2019-04-08    Revert "Fix race condition in deadlineQueue.Dequeue"                                   rcharles@uber.com
* 2019-04-08    Allow passing respool path through aurorabridge flag                                   kevinxu@uber.com
* 2019-04-06    Add debugging log messages at job manager                                              varung@uber.com
* 2019-04-05    Introduce a mini flag in perf test to run just one test                                adityacb@uber.com
* 2019-04-05    Sort recursively executor config keys                                                  varung@uber.com
* 2019-04-05    Break job and update enqueue cycle                                                     zhixin@uber.com
* 2019-04-05    Update yarpc package                                                                   zhixin@uber.com
* 2019-04-04    Increase grpc msg size for bridge grpc client                                          kevinxu@uber.com
* 2019-04-04    Fix ConvertTaskConfigToPodSpec - do not add fields that are not present in TaskConfig to PodSpec sachins@uber.com
* 2019-04-04    Add buckets to syncronize the pod events                                               varung@uber.com
* 2019-04-03    Fix broken 'TestJobCreateWorkflowOnDeletedJobError'                                    sachins@uber.com
* 2019-04-03    Add LogFieldFormatter to add default log fields                                        zhixin@uber.com
* 2019-04-03    Disable job operations after job deletion                                              zhixin@uber.com
* 2019-04-03    Allow stateless.GetJob to accept pod entity version                                    zhixin@uber.com
* 2019-04-03    Add check for nil SLA in GetJobSummary                                                 varung@uber.com
* 2019-04-03    Fix job query for partially created job                                                adityacb@uber.com
* 2019-04-02    Add aurorabridge pod filter                                                            varung@uber.com
* 2019-04-02    Add tests for manual rollback                                                          varung@uber.com
* 2019-04-02    Fix bugs in v1alpha GetPodEvents                                                       sachins@uber.com
* 2019-04-02    Enqueue job to job goal state engine if job state is already KILLED                    varung@uber.com
* 2019-04-02    Fix QueryJobs handler to populate the correct pod stats                                sachins@uber.com
* 2019-04-02    Fix race condition in deadlineQueue.Dequeue                                            zhixin@uber.com
* 2019-04-01    Fix v1alpha GetPodEvents handler to populate pod states instead of task states         sachins@uber.com
* 2019-04-01    Add labels and port config check for task config change                                varung@uber.com
* 2019-04-01    Fix ORM panic when reading a column that is null                                       adityacb@uber.com
* 2019-04-01    Include update message in opaquedata                                                   kevinxu@uber.com
* 2019-04-01    Add switch disable kill tasks request from host manager to mesos master                varung@uber.com
* 2019-04-01    Do not block eventstream when kill of orphan task fails                                apoorvaj@uber.com
* 2019-03-30    Increase QueryJobs request limit                                                       kevinxu@uber.com
* 2019-03-29    Add termination status for update and restart                                          zhixin@uber.com
* 2019-03-29    Fix job level stop and pod level start conflict                                        zhixin@uber.com
* 2019-03-28    Remove job_id tag from SLA metrics                                                     rcharles@uber.com
* 2019-03-28    Use ListJobs for job deletion in aurorabridge integration tests                        kevinxu@uber.com
* 2019-03-28    Add upper bound on http client connection from host manager to mesos master            varung@uber.com
* 2019-03-27    Wait for mesos master leader elected in integration tests                              varung@uber.com
* 2019-03-27    Add SLASpec to JobSummary                                                              varung@uber.com
* 2019-03-27    Correct the logic to check IsCycleCompleted in backoffpolicy                           aihuaxu@uber.com
* 2019-03-26    Fix max_tolerable_instance_failures and max_instances_retries for aurora bridge        kevinxu@uber.com
* 2019-03-26    Bootstrap kafka integration to publish pod events                                      varung@uber.com
* 2019-03-26    Set StartPods flag in UpdateSpec                                                       kevinxu@uber.com
* 2019-03-26    Add aurorabridge abort integ test                                                      varung@uber.com
* 2019-03-25    Add failed update aurora bridge integration test                                       varung@uber.com
* 2019-03-25    Support exclusive placement of tasks to host                                           amitbose@uber.com
* 2019-03-25    Add basic auth in peloton                                                              zhixin@uber.com
* 2019-03-25    Error out on pinned instances                                                          kevinxu@uber.com
* 2019-03-25    Fix uninitialized job recovery                                                         zhixin@uber.com
* 2019-03-25    Add integration test for updating not fully created job                                zhixin@uber.com
* 2019-03-24    Add script to patch v0 api for generated grpc stub                                     yunpeng@uber.com
* 2019-03-22    Workaround bridge client not detecting leadership change                               kevinxu@uber.com
* 2019-03-22    Add support for filtering in the pod watch API                                         apoorvaj@uber.com
* 2019-03-22    Start update affected pods if StartPods flag is set                                    zhixin@uber.com
* 2019-03-22    Remove check of INITIALIZED state upon job update                                      zhixin@uber.com
* 2019-03-21    Generate gRPC stub for peloton                                                         yunpeng@uber.com
* 2019-03-21    Introduced kubernetes in docker for minicluster                                        pourchet@uber.com
* 2019-03-21    Fix performance benchmarking script                                                    avyas@uber.com
* 2019-03-20    Store task labels in the cache                                                         apoorvaj@uber.com
* 2019-03-20    Fix pod state mapping in bridge                                                        kevinxu@uber.com
* 2019-03-20    Fix minicluster script for teardown.                                                   kevinxu@uber.com
* 2019-03-20    Improve performance for integration test runtime                                       pourchet@uber.com
* 2019-03-20    Add common pod level label for all bridge jobs                                         pourchet@uber.com
* 2019-03-20    Convert task stats to pod stats correctly                                              pourchet@uber.com
* 2019-03-20    Attempt to make binary encoded TaskConfig consistent across deploys.                   pourchet@uber.com
* 2019-03-20    Add changelog for release 0.8.2.1                                                      pourchet@uber.com
* 2019-03-20    Fix workflow fields population                                                         pourchet@uber.com
* 2019-03-20    Fixing minicluster script with -a option                                               pourchet@uber.com
* 2019-03-20    add --net=host parameter to development guide                                          pourchet@uber.com
* 2019-03-20    Add compatibility labels for Aurora metadata.                                          pourchet@uber.com
* 2019-03-20    Redundant glide install to fix test flakiness                                          pourchet@uber.com
* 2019-03-20    Add in-place update integration test                                                   pourchet@uber.com
* 2019-03-20    Slight refactor of minicluster code                                                    pourchet@uber.com
* 2019-03-16    InstanceCount from GetJobs endpoint should only include running instances.             kevinxu@uber.com
* 2019-03-15    Fix `ConvertJobSpecToJobConfig` - add missing 'MaximumUnavailableInstances' field      sachins@uber.com
* 2019-03-14    Skip env_name validation when custom executor is used                                  kevinxu@uber.com
* 2019-03-13    Fix KillTasks to kill all tasks when instances is passed as None                       kevinxu@uber.com
* 2019-03-13    [vcluster] Fix reading the prod config for `placement_stateless`                       avyas@uber.com
* 2019-03-12    Fix GetJobUpdateDiffResult nil cases                                                   kevinxu@uber.com
* 2019-03-12    Fixes regarding GetJobUpdateDetails endpoint                                           kevinxu@uber.com
* 2019-03-12    Update vcluster to use `peloton_apps_config_path` for setup only                       avyas@uber.com
* 2019-03-12    Add support to watch task deletion event                                               apoorvaj@uber.com
* 2019-03-12    Include instance events in GetJobUpdateDetails                                         codyg@uber.com
* 2019-03-12    Fix the restart integration tests after the restart-spec change is now released        apoorvaj@uber.com
* 2019-03-12    Reduce the number of instances per peloton daemon in minicluster                       apoorvaj@uber.com
* 2019-03-12    Implement Watch API for pod                                                            kevinxu@uber.com
* 2019-03-12    PE tries to place a task on desired host until a certain deadline                      zhixin@uber.com
* 2019-03-12    Remove held on task when task is killed while not launched                             zhixin@uber.com
* 2019-03-11    Periodically clean up hosts in HeldHost state                                          zhixin@uber.com
* 2019-03-11    Add partially created service job state determiner                                     varung@uber.com
* 2019-03-11    Adjust resource settings for aurorabridge integration tests in read path               kevinxu@uber.com
* 2019-03-11    Launch with invalid offer error is a system error and tasks should be restarted if they receive system error apoorvaj@uber.com
* 2019-03-08    Fix vcluster's use of peloton cluster config path                                      avyas@uber.com
* 2019-03-08    Clean up markdown files in /docs for readthedocs.io                                    min@uber.com
* 2019-03-08    Move to all golang packages to /pkg dir                                                min@uber.com
* 2019-03-07    Fix performance compare script to use production config                                avyas@uber.com

0.8.2.1
------------------
* 2019-03-11    Fix vcluster's use of peloton cluster config path                                      avyas@uber.com

0.8.2
------------------
* 2019-03-06    Removing peloton client and m3 requirement from bootstraping peloton and adding to integration testsmabansal@uber.com
* 2019-03-06    Change performance tests to use `PelotonPerformance` resource pool                    avyas@uber.com
* 2019-03-06    Increase retry durations to prevent from overloading mesos master                     apoorvaj@uber.com
* 2019-03-06    Add metrics for in-place update success rate                                          zhixin@uber.com
* 2019-03-06    Update Version and PrevVersion format in WorkflowStatus                               yuweit@uber.com
* 2019-03-05    Add ability to aggregate SLA metrics per job                                          avyas@uber.com
* 2019-03-05    Integ test revocable job aurora bridge                                                varung@uber.com
* 2019-03-05    Integ test auto rollback aurora bridge                                                varung@uber.com
* 2019-03-05    Enable in-place update/restart feature                                                zhixin@uber.com
* 2019-03-04    Add integration test for ListJob and ListPod API                                      apoorvaj@uber.com
* 2019-03-04    Add access logging for all endpoints                                                  kevinxu@uber.com
* 2019-03-04    Set desired host to handle in-place update overwritten                                zhixin@uber.com
* 2019-03-04    Add canary test framework                                                             varung@uber.com
* 2019-03-04    Put host on held for in-place update                                                  zhixin@uber.com
* 2019-03-04    Clean up aurorabridge integration tests                                               codyg@uber.com
* 2019-03-01    Map ScheduleStatus "THROTTLED" to "FAILED" to workaround error                        kevinxu@uber.com
* 2019-03-01    Fix production config path for placement_[stateless|stateful] process                 kevinxu@uber.com
* 2019-03-01    Add dummy result for write aurorabridge endpoints                                     varung@uber.com
* 2019-02-28    Use empty slices instead of nil in aurorabridge handler                               codyg@uber.com
* 2019-02-28    Fix getTasksWithoutConfigs status filtering                                           kevinxu@uber.com
* 2019-02-28    Fix resource manager bug with draining hosts for maintenance                          avyas@uber.com
* 2019-02-28    Follow up to aurora bridge read path integration tests                                kevinxu@uber.com
* 2019-02-27    Fixing production config read for docker                                              mabansal@uber.com
* 2019-02-27    Change aurorabridge port from 8082 to 5396                                            codyg@uber.com
* 2019-02-27    Delete job if in PENDING state                                                        varung@uber.com
* 2019-02-27    Change Timer to Histogram for SLA measurement                                         avyas@uber.com
* 2019-02-26    Fixes regarding AuroraBridge job labels                                               kevinxu@uber.com
* 2019-02-26    Add HTTP health check support to Peloton                                              apoorvaj@uber.com
* 2019-02-26    Remove link to phab in bridge integration test                                        kevinxu@uber.com
* 2019-02-26    Fixing documents                                                                      mabansal@uber.com
* 2019-02-26    Adding slack channel to readme                                                        mabansal@uber.com
* 2019-02-26    [resource manager] API handler should be blocked until recovery is completed          avyas@uber.com
* 2019-02-26    Remove more uber internal links                                                       min@uber.com
* 2019-02-25    Add integration tests for aurorabridge read path                                      kevinxu@uber.com
* 2019-02-25    Fix the `docker-push` script                                                          avyas@uber.com
* 2019-02-25    Cassandra ORM Secret migration.                                                       yuweit@uber.com
* 2019-02-25    Remove internal docker registry references                                            avyas@uber.com
* 2019-02-22    Fix version used for workflow actions in aurorabridge                                 codyg@uber.com
* 2019-02-22    Removing example files and adding them into client repo                               mabansal@uber.com
* 2019-02-22    Wait for deletion to finish for each aurora bridge integration test                   kevinxu@uber.com
* 2019-02-22    Fix exponential backoff for failed task retry                                         zhixin@uber.com
* 2019-02-22    Add PodEvents object to ORM                                                           sishi@uber.com
* 2019-02-21    Removing arc config from the repo                                                     mabansal@uber.com
* 2019-02-21    Removing uberinternal references from code                                            mabansal@uber.com
* 2019-02-20    Add JobConfigOps to ORM code                                                          adityacb@uber.com
* 2019-02-20    Removing production configs from peloton repo                                         mabansal@uber.com
* 2019-02-19    Add jobType field to task cache                                                       sachins@uber.com
* 2019-02-19    Use yaml file for aurora job configs                                                  varung@uber.com
* 2019-02-19    Fix regression in cached job Delete                                                   adityacb@uber.com
* 2019-02-15    Fix job name to job id mapping being created multiple times                           kevinxu@uber.com
* 2019-02-15    Add instance workflow events with list job updates                                    varung@uber.com
* 2019-02-15    Add performance benchmark tests for stateless                                         apoorvaj@uber.com
* 2019-02-15    Change minicluster components to talk to each other via local container ip            kevinxu@uber.com
* 2019-02-14    Add support for job_name_to_id to ORM                                                 adityacb@uber.com
* 2019-02-14    Cleanup of v1alpha APIs                                                               apoorvaj@uber.com
* 2019-02-14    Write first aurorabridge integration test w/ utils                                    codyg@uber.com
* 2019-02-14    Delete stateless job ID from active jobs list on delete                               adityacb@uber.com
* 2019-02-14    Update push registry in prime                                                         evelynl@uber.com
* 2019-02-14    Populate reason field for deadline exceeded tasks                                     backer@uber.com
* 2019-02-13    Migrate peloton build process to uBuild                                               evelynl@uber.com
* 2019-02-13    Remove container spec from controller integration test                                avyas@uber.com
* 2019-02-12    Support GetAll ORM operation                                                          adityacb@uber.com
* 2019-02-12    Remove dual writes for task_config                                                    adityacb@uber.com
* 2019-02-12    Add additional checks for "not-found" error in aurorabridge                           kevinxu@uber.com
* 2019-02-12    Fix peloton tutorial documents                                                        avyas@uber.com
* 2019-02-11    Re-implement GetJobUpdateDetails / GetJobUpdateSummaries w/ rollback handling         codyg@uber.com
* 2019-02-09    Fix document links in README.md                                                       min@uber.com
* 2019-02-09    Restructure Peloton documentation                                                     min@uber.com
* 2019-02-09    Add doc for Cli section                                                               sishi@uber.com
* 2019-02-07    Implement thermos executor in aurora bridge update path.                              kevinxu@uber.com
* 2019-02-07    Add more information about tasks which breach SLAs                                    avyas@uber.com
* 2019-02-06    Handle new job in GetJobUpdateDiff                                                    codyg@uber.com
* 2019-02-06    Use ORM for mutations to job_index table                                              amitbose@uber.com
* 2019-02-06    Move flag into jobmgr config                                                          sishi@uber.com
* 2019-02-06    Maintain a map of entity version to task count for stateless jobs in job status       apoorvaj@uber.com
* 2019-02-05    Add integration tests for starting/stopping job with an active update                 sachins@uber.com
* 2019-02-05    Fix host filter for revocable tasks at mimir placement engine                         varung@uber.com
* 2019-02-05    Force cache recalculation if jobRuntimeCalculationViaCache flag is on                 sishi@uber.com
* 2019-02-05    Support update metadata                                                               codyg@uber.com
* 2019-02-04    Placement Engine prioritizes host with desired host name                              zhixin@uber.com
* 2019-02-04    Support new CreateJob API in startJobUpdate                                           codyg@uber.com
* 2019-02-04    Only start rollback as paused if awaiting pulse                                       codyg@uber.com
* 2019-02-03    Provide the correct option to vcluster from perf-compare                              amitbose@uber.com
* 2019-02-03    Report integer division fix.                                                          yuweit@uber.com
* 2019-02-02    Remove Uber-specific details from vcluster                                            amitbose@uber.com
* 2019-02-01    Use opaque data to get job update action                                              varung@uber.com
* 2019-02-01    Fix rollback update spec                                                              codyg@uber.com
* 2019-02-01    Implement TODO fields in AuroraBridge.                                                kevinxu@uber.com
* 2019-02-01    Add create control flags in stateless job create API                                  zhixin@uber.com
* 2019-02-01    Modify job config generation test script                                              sishi@uber.com
* 2019-02-01    Change getTasksWithoutConfigs to include pods from previous run                       kevinxu@uber.com
* 2019-02-01    Pass in-place update hint between components                                          zhixin@uber.com
* 2019-01-31    Install Jinja2 package in vcluster.                                                   yuweit@uber.com
* 2019-01-31    stage                                                                                 yuweit@uber.com
* 2019-01-31    Add ability to pass Cassandra username & password to `migrate-db-schema` script       rcharles@uber.com
* 2019-01-31    Use secrets.yaml file to read Peloton langley secrets                                 adityacb@uber.com
* 2019-01-31    Job stop would stop new instances added in update                                     zhixin@uber.com
* 2019-01-31    Stateless job creation is processed by update workflow                                zhixin@uber.com
* 2019-01-31    Allow backfill of active jobs only during resmgr recovery                             adityacb@uber.com
* 2019-01-30    Clear previous contents of maintenanceHostInfoMap when reconciling maintenance state  sachins@uber.com
* 2019-01-29    Implement getJobUpdateDetails                                                         varung@uber.com
* 2019-01-29    Add rollback state handling in NewJobUpdateStatus                                     codyg@uber.com
* 2019-01-29    Add update lifecycle docs                                                             zhixin@uber.com
* 2019-01-29    Fix state machine errors when transition is a no-op                                   avyas@uber.com
* 2019-01-28    Do not set goal state on task initialize                                              sachins@uber.com
* 2019-01-28    [resource manager] Remove default preemption duration from code                       avyas@uber.com
* 2019-01-27    Improve Performance report layout                                                     yuweit@uber.com
* 2019-01-25    Set completion time for all terminated task event                                     zhixin@uber.com
* 2019-01-25    Remove the use of EnqueueGangs from placement engine                                  avyas@uber.com
* 2019-01-25    Implement RollbackJobUpdate                                                           codyg@uber.com
* 2019-01-24    Factor out aurorabridge concurrency into utility func                                 codyg@uber.com
* 2019-01-24    Cherrypick the env variable set.                                                      yuweit@uber.com
* 2019-01-24    Add Flag `task_preemption_period` to Resource Manager                                 yuweit@uber.com
* 2019-01-24    Enforce update id checks on all update actions                                        codyg@uber.com
* 2019-01-24    Implement GetTierConfigs                                                              kevinxu@uber.com
* 2019-01-24    Fix error code being returned in Task.Get()                                           adityacb@uber.com
* 2019-01-23    Follow up changes for getTasksWithoutConfigs                                          kevinxu@uber.com
* 2019-01-23    Implement KillTasks                                                                   codyg@uber.com
* 2019-01-23    Changelog for release-0.8.1                                                           sachins@uber.com
* 2019-01-23    Unify stateless handler non leader handle and error                                   zhixin@uber.com

0.8.1
------------------
* 2019-01-23    Fixing merge conflict                                                                  kevinxu@uber.com
* 2019-01-23    Implement GetJobs                                                                      kevinxu@uber.com
* 2019-01-23    Remove partial job key usage                                                           codyg@uber.com
* 2019-01-23    Remove materialized view `mv_respool_by_owner`                                         avyas@uber.com
* 2019-01-22    Reset hostname when task is reinitialized                                              zhixin@uber.com
* 2019-01-22    Refactor workflow info & workflow status                                               varung@uber.com
* 2019-01-22    Use `testify.suite` library in `launcher_test.go`.                                     yuweit@uber.com
* 2019-01-22    Lazily bootstrap respool in aurorabridge                                               codyg@uber.com
* 2019-01-22    Add support for ListPods API                                                           apoorvaj@uber.com
* 2019-01-22    address comments.                                                                      yuweit@uber.com
* 2019-01-22    Add opaquedata package for simulated unsupported update actions                        codyg@uber.com
* 2019-01-22    Add aurorabridge deploy                                                                varung@uber.com
* 2019-01-22    Implement GetJobSummary                                                                kevinxu@uber.com
* 2019-01-22    Add documentation on how to submit peloton jobs                                        apoorvaj@uber.com
* 2019-01-22    Add doc for types of jobs supported in peloton.                                        apoorvaj@uber.com
* 2019-01-18    Get taskStats from cache and add logging/metric on re-calculate job runtime from cache sishi@uber.com
* 2019-01-18    Implement getJobUpdateDiff                                                             varung@uber.com
* 2019-01-18    Implement getConfigSummary                                                             varung@uber.com
* 2019-01-18    [Resource Manager] Refactor resource tree to not be a singleton                        avyas@uber.com
* 2019-01-17    Fix styling nits from secret formatter code                                            adityacb@uber.com
* 2019-01-17    Fix isJobStateStale                                                                    zhixin@uber.com
* 2019-01-17    Always enqueue ongoing update for stateless jobs                                       sachins@uber.com
* 2019-01-17    Dump jobmgr logs in integration tests only on test failure                             sachins@uber.com
* 2019-01-17    Fix ConvertPodSpecToTaskConfig to not panic on empty containers                        sachins@uber.com
* 2019-01-17    Validate new job spec in JobService.ReplaceJob v1alpha API                             sachins@uber.com
* 2019-01-17    Implement pulseJobUpdate w/ TODO                                                       codyg@uber.com
* 2019-01-16    Make jobs created by performance tests non-preemptile.                                 yuweit@uber.com
* 2019-01-16    Refactor placemenet engine log level settings                                          avyas@uber.com
* 2019-01-16    Migrate stateless_job_test/test_job_workflow to v1alpha                                sachins@uber.com
* 2019-01-16    Use interface methods to publish metrics                                               zhixin@uber.com
* 2019-01-16    Job config documentation                                                               amitbose@uber.com
* 2019-01-16    Check invalid tasks when dequeueing for placing                                        avyas@uber.com
* 2019-01-16    Make generte-protobuf.py run on python2                                                echung@uber.com
* 2019-01-15    Add scaffolding code to help ORM migration                                             adityacb@uber.com
* 2019-01-15    Add integration tests for v1alpha JobService.DeleteJob                                 sachins@uber.com
* 2019-01-15    Fix RestartJob - update JobState on restarting KILLED job                              sachins@uber.com
* 2019-01-15    Adding architecture section                                                            mabansal@uber.com
* 2019-01-15    Add aurora auth support for deploy script                                              adityacb@uber.com
* 2019-01-15    Add job update state change events                                                     varung@uber.com
* 2019-01-15    Update Cli to read config from a well defined set of paths                             sishi@uber.com
* 2019-01-15    Implement getJobUpdateSummaries                                                        varung@uber.com
* 2019-01-15    Second attempt at fixing the placement integration test                                avyas@uber.com
* 2019-01-15    Add aurorabridge/fixture documentation                                                 codyg@uber.com
* 2019-01-15    Fix integration tests                                                                  codyg@uber.com
* 2019-01-14    Implement task/pod termination status                                                  amitbose@uber.com
* 2019-01-14    Add engdocs for Peloton Clients                                                        adityacb@uber.com
* 2019-01-14    Address issues in v1alpha JobService.DeleteJob                                         sachins@uber.com
* 2019-01-14    Use SetPlacment API to return failed placements                                        avyas@uber.com
* 2019-01-14    Make aurorabridge bootstrapper retry until resmgr leader found                         codyg@uber.com
* 2019-01-11    Add docs for job and task lifecycle                                                    avyas@uber.com
* 2019-01-11    Add doc for host maintenance                                                           sachins@uber.com
* 2019-01-11    Write aurorabridge integration test client                                             codyg@uber.com
* 2019-01-11    Add section for API reference                                                          amitbose@uber.com
* 2019-01-11    Add security engdocs for secrets management feature                                    adityacb@uber.com
* 2019-01-11    Adding introduction section and fixing the eng doc format                              mabansal@uber.com
* 2019-01-11    Do not persist default config into db if not exist                                     zhixin@uber.com
* 2019-01-10    Implement GetTasksWithoutConfigs                                                       kevinxu@uber.com
* 2019-01-10    Migrate stateless job start/stop to v1alpha                                            sachins@uber.com
* 2019-01-10    Fix style nits in ORM code                                                             adityacb@uber.com
* 2019-01-10    Removing rst files and adding md files                                                 mabansal@uber.com
* 2019-01-10    Renaming pcluster to minicluster                                                       mabansal@uber.com
* 2019-01-10    Fix flaky test_auto_rollback_reduce_instances                                          zhixin@uber.com
* 2019-01-10    Fix flaky test__failed_task_throttled_by_exponential_backoff                           zhixin@uber.com
* 2019-01-10    Fix err handling for task delete timeout                                               zhixin@uber.com
* 2019-01-09    Fix integration test for placement engine                                              avyas@uber.com
* 2019-01-09    Restart checks if goal state is terminal                                               zhixin@uber.com
* 2019-01-09    Add TerminationStatus to task/pod runtime status                                       amitbose@uber.com
* 2019-01-09    Delete unused benchmark configs                                                        min@uber.com
* 2019-01-09    Adapt building script for new build machines                                           rcharles@uber.com
* 2019-01-09    Move mimir lib to placement/plugins/mimir/lib                                          min@uber.com
* 2019-01-09    Add integration tests for task query.                                                  yuweit@uber.com
* 2019-01-08    Move aurora thrift file into aurorabridge folder                                       kevinxu@uber.com
* 2019-01-08    Enqueue job when task delete                                                           zhixin@uber.com
* 2019-01-08    Add failure message along with reason in integration test                              avyas@uber.com
* 2019-01-08    Set PodSpec.PodName in QueryPodsResponse                                               sachins@uber.com
* 2019-01-08    Implement JobService.DeleteJob v1aplha API                                             sachins@uber.com
* 2019-01-08    Set PodSpec.PodName in GetPodResponse                                                  sachins@uber.com
* 2019-01-08    Disable log dump in integration tests on cluster teardown                              sachins@uber.com
* 2019-01-08    Fix job update performance test timing issue                                           sishi@uber.com
* 2019-01-07    Clean up aurorabridge labels                                                           codyg@uber.com
* 2019-01-07    Add Apache license to source files                                                     min@uber.com
* 2019-01-07    Revert "Fix command print."                                                            min@uber.com
* 2019-01-07    Update integ test framework to show failure reason when a job fails                    avyas@uber.com
* 2019-01-07    Implement get job ids from aurora task query                                           kevinxu@uber.com
* 2019-01-06    Implement AbortJobUpdate                                                               codyg@uber.com
* 2019-01-04    Keep more logs for Peloton components in vcluster                                      amitbose@uber.com
* 2019-01-04    Change git repo name to github.com/uber/peloton                                        min@uber.com
* 2019-01-04    Migrate tests in stateless_job_test/test_update.py to v1 alpha API                     zhixin@uber.com
* 2019-01-04    Implement ResumeJobUpdate                                                              codyg@uber.com
* 2019-01-04    Add go runtime metrics                                                                 avyas@uber.com
* 2019-01-03    Improve logging in perf tests                                                          amitbose@uber.com
* 2019-01-03    Implement PauseJobUpdate                                                               codyg@uber.com
* 2019-01-03    Add changelog 0.8.0                                                                    varung@uber.com
* 2019-01-03    Implement startJobUpdate                                                               codyg@uber.com
* 2019-01-03    Add GetJobUpdate API                                                                   varung@uber.com
* 2019-01-03    Update API docs                                                                        avyas@uber.com
* 2019-01-03    Implement JobService.StartJob v1alpha API                                              sachins@uber.com

0.8.0
------------------

* 2019-01-02    Fix SLA tracking for tasks in resmgr                                                   avyas@uber.com
* 2019-01-02    Update to token aware host policy with dc aware round robin                            varung@uber.com
* 2019-01-02    Add engdocs for preemption                                                             avyas@uber.com
* 2019-01-02    Fix revocable integ test typo error                                                    varung@uber.com
* 2019-01-02    Implement JobService.QueryPods v1alpha API                                             sachins@uber.com
* 2018-12-31    Ensure vcluster is cleaned-up when perf test fails                                     amitbose@uber.com
* 2018-12-31    Improve vcluster error handling                                                        amitbose@uber.com
* 2018-12-31    vcluster changes for stateless jobs                                                    amitbose@uber.com
* 2018-12-31    Migrate tests in stateless_job_test/test_job_revocable.py to v1 alpha API              zhixin@uber.com
* 2018-12-31    Fix unit test for orm client                                                           adityacb@uber.com
* 2018-12-31    Migrate tests in stateless_job_test/test_job.py to v1 alpha API                        zhixin@uber.com
* 2018-12-28    Update golang version to 1.11.4                                                        avyas@uber.com
* 2018-12-28    Migrate part of stateless job integration tests to v1 alpha api                        zhixin@uber.com
* 2018-12-28    Implement ORM update functionality                                                     adityacb@uber.com
* 2018-12-28    Implement ListJobUpdates                                                               zhixin@uber.com
* 2018-12-28    Add integration test for task level preemption                                         avyas@uber.com
* 2018-12-27    Fix review comments in ORM delete path                                                 adityacb@uber.com
* 2018-12-27    Wire up ExecutorInfo in ContainerSpec when calling v1 Create/ReplaceJob api            kevinxu@uber.com
* 2018-12-27    Implement ORM Delete functionality                                                     adityacb@uber.com
* 2018-12-26    Fix job cleanup for revocable integ test                                               varung@uber.com
* 2018-12-26    Add implementation to read/write workflow events per instance                          varung@uber.com
* 2018-12-26    Add sla tracking config to deploy script                                               avyas@uber.com
* 2018-12-26    Get task configs from task_config_v2 first                                             adityacb@uber.com
* 2018-12-26    Fix update-protobuf gens                                                               varung@uber.com
* 2018-12-24    Remove -u flag from packr installation                                                 sishi@uber.com
* 2018-12-23    Implement job restart                                                                  zhixin@uber.com
* 2018-12-21    Part III: Setup recovery from active_jobs table                                        adityacb@uber.com
* 2018-12-21    Implement JobSVC.Stop                                                                  zhixin@uber.com
* 2018-12-20    Add GetJob CLI and unit tests                                                          sachins@uber.com
* 2018-12-20    Implement JobService.CreateJob API                                                     sachins@uber.com
* 2018-12-19    Support Thermos Executor in Peloton                                                    kevinxu@uber.com
* 2018-12-19    Re-generate Aurora thrift bindings with thriftrw dev branch                            codyg@uber.com
* 2018-12-19    GetPod should populate the right PodSpec for previous runs                             sachins@uber.com
* 2018-12-19    Add support to store opaque data along with an update                                  apoorvaj@uber.com
* 2018-12-19    Add handler implementation for job name to job id                                      varung@uber.com
* 2018-12-19    Use `PREEMPTING` as goal state in job manager for batch                                avyas@uber.com
* 2018-12-18    Honor retry policy for lost tasks                                                      amitbose@uber.com
* 2018-12-18    Add PERF_version.no_(BASE | CURRENT) to Performance Report table.                      yuweit@uber.com
* 2018-12-18    Generate yarpc binding for aurora thrift api                                           varung@uber.com
* 2018-12-18    Ignore UNITILIAZED job without config upon recovery                                    zhixin@uber.com
* 2018-12-17    Fix performance test version issues.                                                   yuweit@uber.com
* 2018-12-17    Use deadline queue in the goal state engine as the queue serving the worker pool       apoorvaj@uber.com
* 2018-12-17    Add ReplaceJobDiff API to find the instances being added/removed/updated in a replace API invocation apoorvaj@uber.com
* 2018-12-17    Fix archiver flag, and context timeout issue                                           adityacb@uber.com
* 2018-12-17    Add get job id from job name interface definition                                      varung@uber.com
* 2018-12-14    Add support for task level preemption                                                  avyas@uber.com
* 2018-12-14    Feature flag to calculate job runtime from cache if MV diverged                        sishi@uber.com
* 2018-12-14    Manually patch glide.lock to correct commit id for go-internal                         adityacb@uber.com
* 2018-12-14    Implement v1alpha ListJobs streaming API                                               apoorvaj@uber.com
* 2018-12-13    Add znode for peloton-aurora-bridge to mock Aurora Leader                              varung@uber.com
* 2018-12-13    Change archiver stream_only_mode flag default value                                    adityacb@uber.com
* 2018-12-13    Upgrade testify to 1.2.0                                                               codyg@uber.com
* 2018-12-13    Part II: Recover jobs from active_jobs table                                           adityacb@uber.com
* 2018-12-13    Add pause/resume/abort job workflow                                                    zhixin@uber.com
* 2018-12-12    Get zk configuration file into go binary                                               sishi@uber.com
* 2018-12-12    In Perf tests, Wait 30 Sec Before Creating Respool.                                    yuweit@uber.com
* 2018-12-12    Add task_config_v2 table partitioned by job_id, version, instance_id                   adityacb@uber.com
* 2018-12-10    Implement stateless service QueryJobs                                                  zhixin@uber.com
* 2018-12-10    Fix bin_packing setting in deploy script                                               adityacb@uber.com
* 2018-12-10    Handle hostmgr bin packing flag in deploy script                                       adityacb@uber.com
* 2018-12-10    Create task configs in parallel                                                        sachins@uber.com
* 2018-12-10    Add ReplaceJob handler                                                                 zhixin@uber.com
* 2018-12-10    Scaffold aurorabridge daemon                                                           codyg@uber.com
* 2018-12-10    Add short term fixes to improve performance of SLA tracking                            avyas@uber.com
* 2018-12-07    Add two tables (job.Get and job.Update) to Peloton performance report.                 yuweit@uber.com
* 2018-12-06    Add storage APIs for job id from job name                                              varung@uber.com
* 2018-12-06    Implement v1alpha GetJob API                                                           apoorvaj@uber.com
* 2018-12-05    Add PodService.DeletePodEvents handler                                                 sachins@uber.com
* 2018-12-04    Add entity version validation to workflow operations                                   zhixin@uber.com
* 2018-12-04    Add APIs to fetch workflow information and events                                      apoorvaj@uber.com
* 2018-12-03    Add CLI support to list all the available hosts                                        kevinxu@uber.com
* 2018-12-03    Enhance optional arguments for customized pcluster usage                               kulkarni@uber.com
* 2018-12-03    Add changelog for 0.7.8.1                                                              avyas@uber.com
* 2018-12-03    Support starting an update in paused state                                             apoorvaj@uber.com
* 2018-12-03    Disable bin packing by default                                                         avyas@uber.com
* 2018-11-30    Move Requeue of un-placed tasks inside rmtask                                          avyas@uber.com
* 2018-11-30    Create performance test for batch job update                                           sishi@uber.com
* 2018-11-30    Reenable job factory metrics                                                           zhixin@uber.com
* 2018-11-30    Fix data race when job factory publish metrics                                         zhixin@uber.com
* 2018-11-30    Refactor JobMgr cache to guard workflow cache by job cache lock                        zhixin@uber.com
* 2018-11-30    Storage Architecture v2 first cut                                                      adityacb@uber.com
* 2018-11-30    Add job_name to job_id mapping                                                         varung@uber.com
* 2018-11-29    [Refactor] Job's wait_for_condition method                                             yuweit@uber.com
* 2018-11-29    Part I: Use active_jobs table instead of mv_jobs_by_state for recovery                 adityacb@uber.com
* 2018-11-29    Add changelog for 0.7.8                                                                avyas@uber.com

0.7.8.1
------------------
* 2018-12-03    Disable bin packing by default                                                      avyas@uber.com

0.7.8
------------------
* 2018-11-27    Update job index when job configuration is updated                                            apoorvaj@uber.com
* 2018-11-28    Revert "Constraint job and task configurations at DB"                                         varung@uber.com
* 2018-11-28    Revert "Converge recent job config version for all batch tasks"                               varung@uber.com
* 2018-11-28    Move entity version construct into util                                                       zhixin@uber.com
* 2018-11-28    Do not untrack stateless jobs from the cache                                                  apoorvaj@uber.com
* 2018-11-28    Revert "Batch job of INITIALIZED state can be updated"                                        varung@uber.com
* 2018-11-28    Revert "Reenable job factory metrics"                                                         varung@uber.com
* 2018-11-28    Add PodService.GetPod handler                                                                 sachins@uber.com
* 2018-11-21    Adding asynchronous bin pacing computation support and enable DEFRAG bin packing algorithm    mabansal@uber.com
* 2018-11-28    Add PodService.PodStop handler                                                                sachins@uber.com
* 2018-11-27    Set agent id to pointer of empty string on task launch failure                                zhixin@uber.com
* 2018-11-27    Add actual tasks allocation for Mesos                                                         varung@uber.com
* 2018-11-20    Add integration test for manual rollback                                                      apoorvaj@uber.com
* 2018-11-21    reduce cachedjob.AddTask lock contention                                                      zhixin@uber.com
* 2018-11-20     Use convertTaskStateToPodState to convert task state                                         zhixin@uber.com
* 2018-11-20    Image is reported in the container status, hence it is not needed in pod summary as well      apoorvaj@uber.com
* 2018-11-20    Deprecate write/read path for task_state_changes table                                        varung@uber.com
* 2018-11-07    Add API for multiple containers in a pod.                                                     apoorvaj@uber.com
* 2018-11-16    Fix TaskSVC.GetPodEvents to return events for all runs                                        sachins@uber.com
* 2018-11-16    Add PodService.RestartPod handler                                                             sachins@uber.com
* 2018-11-15    Add PodService.BrowsePodSandbox handler                                                       sachins@uber.com
* 2018-11-15    Add JobSVC.RefreshJob                                                                         zhixin@uber.com
* 2018-11-13    Add JobSvc.GetJobCache                                                                        zhixin@uber.com
* 2018-11-14    Push images to Kraken                                                                         sachins@uber.com
* 2018-11-13    Fix data race issues with respool interface                                                   varung@uber.com
* 2018-11-13    Lookup zk info by providing a cluster name                                                    sishi@uber.com
* 2018-11-13    Update respool doc with revocable tasks                                                       varung@uber.com
* 2018-11-13    Fix JobSvc.Create error when the previous create call fails                                   zhixin@uber.com
* 2018-11-12    Dump host manager event stream via CLI                                                        varung@uber.com
* 2018-11-12    Handle instances with different desired config than their current config                      apoorvaj@uber.com
* 2018-11-06    Add stress test for job create/get to vcluster                                                adityacb@uber.com
* 2018-11-12    Add PodSvc.StartPod                                                                           zhixin@uber.com
* 2018-10-19    Add reason for failure in placement engine                                                    avyas@uber.com
* 2018-10-29    Compress job config before storing it to DB                                                   adityacb@uber.com
* 2018-11-12    Increase runtime of non-preemptible test job                                                  avyas@uber.com
* 2018-11-08    Do not unset the job update identifier after update is complete                               apoorvaj@uber.com
* 2018-11-08    Add CLI command to kill all jobs owned by a given owner                                       sachins@uber.com
* 2018-11-07    Add PodSVC.RefreshPod                                                                         zhixin@uber.com
* 2018-10-31    v1alpha: Define APIs for watching for changes                                                 amitbose@uber.com
* 2018-11-07    Add changelog for 0.7.7.3                                                                     varung@uber.com
* 2018-11-07    Add GetPodEvents API handler                                                                  sachins@uber.com
* 2018-10-23    Handle race conditions during consecutive updates                                             apoorvaj@uber.com
* 2018-11-07    Fix deadlock in resmgr/respool/respool.go                                                     sachins@uber.*com
* 2018-11-07    Fix deadlock in resmgr/respool/respool.go                                                     sachins@uber.com
* 2018-11-06    Fix flaky update integration test                                                             zhixin@uber.com
* 2018-11-06    Implement PodSVC.GetPodCache                                                                  zhixin@uber.com
* 2018-11-05    Add ConvertToYARPCError for pod service handler                                               zhixin@uber.com
* 2018-11-02    Add integration test for pause and resume update                                              zhixin@uber.com
* 2018-08-23    Framework for failure testing                                                                 amitbose
* 2018-11-02    Fix update stuck when instances failed                                                        zhixin@uber.com
* 2018-10-30    v1alpha Add APIs to get all jobs/tasks as a stream                                            amitbose@uber.com
* 2018-11-01    Add integration tests for update and health check                                             zhixin@uber.com
* 2018-11-01    Add sorting by instanceId, name, host, reason for task query                                  sishi@uber.com
* 2018-11-01    Dedupe pending task status update acknowledgement                                             varung@uber.com
* 2018-11-01    Change log level when field not found in object (FillObject)                                  sachins@uber.com
* 2018-11-01    Remove test__host_limit integration test from smoke test list                                 varung@uber.com
* 2018-10-31    Add dummy PodService V1 Alpha API handler                                                     sachins@uber.com
* 2018-10-31    Fix healthState when healthcheck is not enabled                                               zhixin@uber.com
* 2018-10-31    Task uses new task config when reinitialized                                                  zhixin@uber.com
* 2018-10-31    Add dummy JobService V1 Alpha API handler                                                     sachins@uber.com
* 2018-10-30    Fix preemption for revocable tasks                                                            varung@uber.com
* 2018-10-30    Fix v1aplha API by replacing stateless_job with stateless.                                    apoorvaj@uber.com
* 2018-10-29    Calculate slack entitlement for non-revocable resources from main entitlement                 varung@uber.com
* 2018-10-12    Add peloton v1alpha API for stateless jobs.                                                   apoorvaj@uber.com
* 2018-10-26    Fix preemption integration test and add it to default tag                                     avyas@uber.com
* 2018-10-25    Add metrics by tasks state at event stream                                                    varung@uber.com
* 2018-10-26    If cleanup of one update fails, continue cleaning up the remaining                            apoorvaj@uber.com
* 2018-10-25    DELETED goal state cannot be overwritten unless a configuration change is requested           apoorvaj@uber.com
* 2018-10-25    Implement pause and resume for an update                                                      zhixin@uber.com
* 2018-10-25    Add changelog for 0.7.7.2                                                                     rcharles@uber.com
* 2018-10-25    Add changelog for 0.7.7.2                                                                     rcharles@uber.com
* 2018-10-24    Batch job of INITIALIZED state can be updated                                                 zhixin@uber.com
* 2018-10-24    Refactor event status update for clarity                                                      zhixin@uber.com
* 2018-10-24    Batch job of INITIALIZED state can be updated                                                 zhixin@uber.com
* 2018-10-24    Add integration test for stateless revocable job                                              varung@uber.com
* 2018-10-24    Add test for cli output                                                                       avyas@uber.com
* 2018-10-22    Implement reducing instance count using DELETED task goal state                               apoorvaj@uber.com
* 2018-10-22    Remove DRAINING hosts from Cluster Capacity calculation                                       sachins@uber.com
* 2018-10-23    Add unit test for update                                                                      zhixin@uber.com
* 2018-10-22    Add Changelog for 0.7.7.1                                                                     rcharles@uber.com
* 2018-10-22    Add Changelog for 0.7.7.1                                                                     rcharles@uber.com
* 2018-10-17    Ignore start request for non-running tasks                                                    apoorvaj@uber.com
* 2018-10-22    Rollback update on failure                                                                    zhixin@uber.com
* 2018-08-29    Constraint job and task configurations at DB                                                  varung@uber.com
* 2018-10-22    Revert "Restrict the maximum number of updates allowed per job"                               varung@uber.com
* 2018-10-08    Use `HostOfferID` when launching batch and stateless tasks                                    avyas@uber.com
* 2018-10-19    Increase goal state unit test coverage to > 90%                                               apoorvaj@uber.com
* 2018-10-22    Revert "Restrict the maximum number of updates allowed per job"                               varung@uber.com
* 2018-10-22    Add exponential backoff for fail retry                                                        zhixin@uber.com
* 2018-10-19    Maintain all host states in Peloton                                                           sachins@uber.com

0.7.7.3
-----------------
* 2018-11-07    Fix deadlock in resmgr/respool/respool.go                                              sachins@uber.com

0.7.7.2
-----------------
* 2018-10-25    Batch job of INITIALIZED state can be updated                                          zhixin@uber.com

0.7.7.1
-----------------
* 2018-10-22    Constraint job and task configurations at DB                                           rcharles@uber.com
* 2018-10-22    Revert "Restrict the maximum number of updates allowed per job"                        rcharles@uber.com

0.7.7
-----------------
* 2018-10-17    Get rid of glide dependency and fix peloton cli build                                  xiaojian@uber.com
* 2018-10-17    Rename label key for system label resource pool                                        rcharles@uber.com
* 2018-10-17    Converge recent job config version for all batch tasks                                 varung@uber.com
* 2018-10-17    Add KAFKA_TOPIC env var to archiver deploy script                                      adityacb@uber.com
* 2018-10-16    Add filebeat_topic to archiver logs                                                    adityacb@uber.com
* 2018-10-16    Make enable revocable resources env as string for vCluster                             varung@uber.com
* 2018-10-16    Update golint repo path                                                                avyas@uber.com
* 2018-10-16    Fix race condition in `RecoverJobsByState`                                             sachins@uber.com
* 2018-10-16    remove test__create_multiple_consecutive_updates temporarily                           zhixin@uber.com
* 2018-10-15    Cleanup metrics for Host Manager                                                       varung@uber.com
* 2018-10-15    Not finding the entity in the map after dequeue is not an error                        apoorvaj@uber.com
* 2018-10-12    Make it easier to mount local binaries in pcluster                                     amitbose@uber.com
* 2018-10-12    Expose revocable resources attribute as env variable                                   varung@uber.com
* 2018-10-12    Aggregate non-leaf resource pool queues size for metrics                               varung@uber.com
* 2018-10-12    Modify FillObject to fill an object having fewer fields than that in data              sachins@uber.com
* 2018-10-12    Add v1alpha directory as a copy of v0 api.                                             apoorvaj@uber.com
* 2018-10-11    Clean-up resource-manager tasks stuck in LAUNCHING                                     amitbose@uber.com
* 2018-10-11    Fix go get for lint                                                                    amitbose@uber.com
* 2018-10-10    Validate that revocable job is set preemptible                                         varung@uber.com
* 2018-10-09    Carry on update progress when task will not be started                                 zhixin@uber.com
* 2018-10-09    Skip flaky unit test TestAddRunningTasks                                               varung@uber.com
* 2018-10-08    Add admission control for revocable/slack resources                                    varung@uber.com
* 2018-10-08    Construct system labels based on new config for Job Update                             sachins@uber.com
* 2018-10-05    UpdateStatus includes instances failed                                                 zhixin@uber.com
* 2018-10-05    QueryHosts API returns all hosts if request empty                                      sachins@uber.com
* 2018-10-04    Remove client-added system labels at the time of job creation                          sachins@uber.com
* 2018-10-04    Reenable job factory metrics                                                           zhixin@uber.com
* 2018-10-04    Fix InitJobFactory in unit test                                                        zhixin@uber.com
* 2018-10-04    Mark update failed if more instances failed during update than configured              zhixin@uber.com
* 2018-10-04    Add a listener to jobmgr cache                                                         amitbose@uber.com
* 2018-10-03    Ignore same status for healthy update                                                  varung@uber.com
* 2018-10-03    Reduce jobFactory lock contention                                                      zhixin@uber.com
* 2018-10-03    Batch job and stateless job share the same jobStateDeterminer                          zhixin@uber.com
* 2018-10-02    Restrict the maximum number of updates allowed per job                                 apoorvaj@uber.com
* 2018-10-02    Put git commit and build-url in performance test report                                amitbose@uber.com
* 2018-10-02    Add `HostOfferID` to placements                                                        avyas@uber.com
* 2018-10-01    Add entitlement calculation for revocable/slack resources                              varung@uber.com
* 2018-10-01    Use previous run id from persistent storage on adding shrink instances                 varung@uber.com
* 2018-10-01    Disable recording state transition durations for `rmTask`                              avyas@uber.com
* 2018-10-01    Filter pod events by RunID                                                             varung@uber.com
* 2018-10-01    Comment out runPublishMetrics for job factory                                          zhixin@uber.com
* 2018-10-01    Add system labels to peloton tasks                                                     sachins@uber.com
* 2018-09-28    Use mimir for `Stateless` task types                                                   avyas@uber.com
* 2018-09-27    Remove mock datastore test from make unit-test                                         varung@uber.com
* 2018-09-27    Fix flaky unit test TestEngineAssignPortsFromMultipleRanges                            varung@uber.com
* 2018-09-27    Fix flaky unit test for host summary match filter                                      avyas@uber.com
* 2018-09-27    Add get Peloton Version end point                                                      sishi@uber.com
* 2018-09-26    Skip flaky unit test for host summary match filter                                     varung@uber.com
* 2018-09-26    Changelog for release 0.7.6                                                            adityacb@uber.com
* 2018-09-26    Fix merge error                                                                        zhixin@uber.com

0.7.6
-----------------
* 2018-09-26    JobMgr recovers jobs by job type                                                       zhixin@uber.com
* 2018-09-26    Add interation test for stateless job API and fix TaskTerminatedRetry                  zhixin@uber.com
* 2018-09-25    Disabling Dfrag bin packing                                                            mabansal@uber.com
* 2018-09-25    Do not emit error metrics if enqueue gangs fails due to gang already exists            zhixin@uber.com
* 2018-09-25    Add `HostOfferID` to offers from host manager                                          avyas@uber.com
* 2018-09-24    Add parallelism to add offers by host.                                                 varung@uber.com
* 2018-09-22    Fix CompareAndSetRuntime build error                                                   zhixin@uber.com
* 2018-09-21    change cassandra version from 3.9 -> 3.0.14 for local container                        varung@uber.com
* 2018-09-21    Fix flaky unit test to parse instanceID                                                varung@uber.com
* 2018-09-21    Do not abort existing update in the update svc handler                                 apoorvaj@uber.com
* 2018-09-21    Set update identifier to nil in job runtime while untracking the update                apoorvaj@uber.com
* 2018-09-21    Bump Mimir-Lib                                                                         kejlberg@uber.com
* 2018-09-21    Fix command print.                                                                     kejlberg@uber.com
* 2018-09-20    Implement restart overwrite strategy                                                   zhixin@uber.com
* 2018-09-20    Tasks in a terminated batch job cannot be restarted.                                   apoorvaj@uber.com
* 2018-09-20    Fix test setup for a local linux dev laptop.                                           kejlberg@uber.com
* 2018-09-20    Delete Pod Events on Job Delete                                                        varung@uber.com
* 2018-09-20    Add metrics for task state transitions                                                 avyas@uber.com
* 2018-09-19    Add env variable to enable pod events cleanup                                          varung@uber.com
* 2018-09-19    Add CompareAndSetRuntime to support job write after read                               zhixin@uber.com
* 2018-09-18    Fix the build and tests for terminate-retry                                            apoorvaj@uber.com
* 2018-09-18    Add support for handling tasks with id greater than instance count                     apoorvaj@uber.com
* 2018-09-18    Improve storage unit test coverage                                                     adityacb@uber.com
* 2018-09-18    Removing empty test file from YARPC package                                            mabansal@uber.com
* 2018-09-18    Retry on  terminated long running task                                                 chunyang.shen@uber.com
* 2018-09-17    Add constraint pod events in Archiver                                                  varung@uber.com
* 2018-09-15    Adding Defragmentation bin packing suppport for host manager                           mabansal@uber.com
* 2018-09-14    Move RuntimeDiff and JobConfig from cached to models                                   zhixin@uber.com
* 2018-09-13    Add integration tests for host maintenance                                             sachins@uber.com
* 2018-09-13    Clean up updates and other ops separately                                              zhixin@uber.com
* 2018-09-13    Add job level start and stop support                                                   zhixin@uber.com
* 2018-09-13    Add changelog for 0.7.5.1 and 0.7.5.2                                                  zhixin@uber.com
* 2018-09-12    Update debug commands and remove pressure test                                         sishi@uber.com
* 2018-09-12    Add pod events handle empty desired mesos task id                                      zhixin@uber.com
* 2018-09-12    Fix job runtime updater logged job state                                               zhixin@uber.com
* 2018-09-11    Dump logs when goal state action execute returns an error                              apoorvaj@uber.com
* 2018-09-11    Change logrus version to `1.0.0`                                                       avyas@uber.com
* 2018-09-11    Add support to reduce instance count during an update                                  apoorvaj@uber.com
* 2018-09-10    Fix bug in reading job_index table                                                     adityacb@uber.com
* 2018-09-10    Desired mesos task id should not be nil when comparing                                 apoorvaj@uber.com
* 2018-09-09    Add parallelism to calculate cluster capacity                                          varung@uber.com
* 2018-09-08    Add revocable task launch support to host manager                                      varung@uber.com
* 2018-09-07    Set drainer period for development environment                                         sachins@uber.com
* 2018-09-06    Set hostname field in resmgr tasks only if relevant                                    sachins@uber.com
* 2018-09-06    Add override url for hostmgr in Peloton Client                                         sachins@uber.com
* 2018-09-06    Implement job level Restart                                                            zhixin@uber.com
* 2018-09-06    Reconcile stale job index entries                                                      adityacb@uber.com
* 2018-09-05    Refactor update to support different workflow                                          zhixin@uber.com
* 2018-09-05    Set proper health state for task whenever it re-initiate                               chunyang.shen@uber.com
* 2018-09-04    Add integration tests for update of a stateless job                                    apoorvaj@uber.com
* 2018-09-04    Reconcile active jobs which have out of sync mv_task_by_state entry                    adityacb@uber.com
* 2018-08-31    Add job restart proto and cli                                                          zhixin@uber.com
* 2018-08-31    Add Changelog for 0.7.5 release                                                        rcharles@uber.com
* 2018-08-30    Refactor preemption package to remove singleton and add tests                          avyas@uber.com
* 2018-08-29    Use slave agent IP for fetching logs                                                   amitbose

0.7.5.2
-----------------
* 2018-09-12    Add pod events handle empty desired mesos task id                                      zhixin@uber.com.
* 2018-09-11    Add override url for hostmgr in Peloton Client                                         avyas@uber.com.

0.7.5.1
-----------------
* 2018-09-11    Change logrus version to `1.0.0`                                                       avyas@uber.com.
* 2018-09-11    Desired mesos task id should not be nil when comparing                                 apoorvaj@uber.com.
* 2018-08-31    Add Changelog for 0.7.5 release                                                        rcharles@uber.com.

0.7.5
------------------
* 2018-08-29    Remove unused function in reservation                                                  mu@uber.com
* 2018-08-28    Use desiredMesosTaskID to replace PREEMPTING state for task preemption                 zhixin@uber.com
* 2018-08-28    Update CLI help text for host maintenance commands                                     sachins@uber.com
* 2018-08-28    Fix flaky `TestPeriodicCalculationWhenStarted`                                         avyas@uber.com
* 2018-08-28    addPodEvent set prevRunID to 0 when PrevMesosTaskId not provided                       zhixin@uber.com
* 2018-08-27    Revert "Measure resource pool SLA at a more granular level"                            avyas@uber.com
* 2018-08-27    Allow user to provide configuration version when creating a job update in CLI          apoorvaj@uber.com
* 2018-08-27    Auto-refresh maintenance queue periodically                                            sachins@uber.com
* 2018-08-27    Refactor mesos task ID generation to CreateMesosTaskID                                 zhixin@uber.com
* 2018-08-27    Specify only hostname (no IP) for host maintenance                                     sachins@uber.com
* 2018-08-24    Add desired mesos task id for task and pod events                                      zhixin@uber.com
* 2018-08-24    Add support to query hosts in UP state                                                 sachins@uber.com
* 2018-08-24    Modify AgentMap to have hostname as the key instead of AgentId                         sachins@uber.com
* 2018-08-23    Modify AgentInfoMap to contain more information about the Agent                        sachins@uber.com
* 2018-08-23    Fix job metrics name typo                                                              zhixin@uber.com
* 2018-08-23    CLI default range.To to MaxInt32                                                       zhixin@uber.com
* 2018-08-23    KILLING state with SUCCEEDED/RUNNING goal state is valid                               zhixin@uber.com
* 2018-08-22    Implement task level restart                                                           zhixin@uber.com
* 2018-08-22    Add desired mesos task id in task runtime                                              zhixin@uber.com
* 2018-08-22    Automatically set the configuration version during job update                          apoorvaj@uber.com
* 2018-08-21    Fix resmgr recovery                                                                    sachins@uber.com
* 2018-08-21    Cleanup unused storage code, add tests                                                 adityacb@uber.com
* 2018-08-21    WriteProgress validates state change                                                   zhixin@uber.com
* 2018-08-21    Adding unit tests in common and statemachine package                                   mabansal@uber.com
* 2018-08-20    Fixing flaky unit test                                                                 mabansal@uber.com
* 2018-08-21    Adding unit tests for peloton/cli                                                      apoorvaj@uber.com
* 2018-08-20    Adding unit tests for resource manager handlers                                        mabansal@uber.com
* 2018-08-20    Fix testjob_uber_docker_service                                                        chunyang.shen@uber.com

0.7.4
------------------

* 2018-08-20    Fix 'context' of RestoreMaintenanceQueue call in resmgr/recovery.go                    sachins@uber.com
* 2018-08-20    Instantiate `finished` channel outside of `NewRecovery`                                avyas@uber.com
* 2018-08-20    Allow to disable Prometheus while maintaining metric name format                       rcharles@uber.com
* 2018-08-20    Integrate health check with update process                                             chunyang.shen@uber.com
* 2018-08-17    Increase the memlimit for peloton apps on vcluster                                     avyas@uber.com
* 2018-08-16    Change "total instance count is greater than expected" to debug                        zhixin@uber.com
* 2018-08-16    Fix update integration test                                                            zhixin@uber.com
* 2018-08-15    Make `MarkHostsDrained` API call only for 'DRAINING' hosts                             sachins@uber.com
* 2018-08-15    Unit tests for yarpc/peer                                                              amitbose
* 2018-08-14    Remove executor code from peloton because it is not used                               adityacb@uber.com
* 2018-08-14    Update PodEvent protobuf to use mesosTaskID rather pelotonTaskID                       varung@uber.com
* 2018-08-14    Adding tests for resource manager recovery                                             mabansal@uber.com
* 2018-08-14    Update logrus version to `^1.0.0`                                                      avyas@uber.com
* 2018-08-14    Disable prometheus reporting for resource manager                                      avyas@uber.com
* 2018-08-14    Add integration test for update                                                        zhixin@uber.com
* 2018-08-13    Enable archiver streaming only mode, add unit tests                                    adityacb@uber.com
* 2018-08-13    Removing interfaces from hostmanager reserver and cleanup code                         mabansal@uber.com
* 2018-08-13    [API] Set task kill grace period per each task                                         adityacb@uber.com
* 2018-08-13    UpdateRun only checks currently updating instances                                     zhixin@uber.com
* 2018-08-13    Restore host->tasks map on resmgr recovery                                             sachins@uber.com
* 2018-08-12    Dual write state transitions to pod_events and task_events for batch jobs              varung@uber.com
* 2018-08-10    Check nil for cached job                                                               zhixin@uber.com
* 2018-08-09    Measure resource pool SLA at a more granular level                                     avyas@uber.com
* 2018-08-09    Add UNKNOWN and DISABLED into health state and persist health state into pod_events table chunyang.shen@uber.com
* 2018-08-09    Add unit-tests for package util                                                        amitbose
* 2018-08-09    Add unit-tests for package common/background                                           amitbose
* 2018-08-09    Terminated task with correct desired configuration is update complete                  zhixin@uber.com
* 2018-08-08    Restore host->tasks map on resmgr recovery                                             sachins@uber.com
* 2018-08-08    Add tests for `resmgr/server.go`                                                       avyas@uber.com
* 2018-08-06    Improve code coverage for common/logging                                               varung@uber.com
* 2018-08-06    Update the task healthy field only when event reason is REASON_TASK_HEALTH_CHECK_STATUS_UPDATED chunyang.shen@uber.com
* 2018-08-06    Make vcluster run on dca1-preprod01                                                    amitbose
* 2018-08-06    Add unit test for tasksvc start and stop                                               zhixin@uber.com
* 2018-08-03    Add tests for reconciler                                                               avyas@uber.com
* 2018-08-03    Revert "Revert "Record state transition durations for RMTask""                         avyas@uber.com
* 2018-08-03    Add comments for test coverage of JobMgr                                               chunyang.shen@uber.com
* 2018-08-03    Add job configuration validation for different type of job                             chunyang.shen@uber.com
* 2018-08-02    Add CLI to get Pod Events                                                              varung@uber.com
* 2018-08-02    Increase code coverage for hostmgr/reconcile                                           sachins@uber.com
* 2018-08-02    JobRuntimeUpdater treats job with update as non-paritally-created job                  zhixin@uber.com
* 2018-08-02    Set task goal state to KILLED for terminated task                                      zhixin@uber.com
* 2018-08-02    Handle errors during update create                                                     apoorvaj@uber.com
* 2018-08-02    Add unit test for tasksvc get                                                          zhixin@uber.com
* 2018-08-02    Add unit test for jobmgr/tak/placement                                                 zhixin@uber.com
* 2018-08-02    Add unit test for volumesvc                                                            zhixin@uber.com
* 2018-08-02    Add unit test for tasksvc get events                                                   zhixin@uber.com
* 2018-08-02    Increase code coverage for hostmgr/                                                    sachins@uber.com
* 2018-08-01    Increase code coverage for hostmgr/hostsvc                                             sachins@uber.com
* 2018-08-01    Increase code coverage for hostmgr/queue                                               sachins@uber.com
* 2018-08-01    Implement update pause on server side                                                  zhixin@uber.com
* 2018-08-01    Do not start goal state engines and preemptor before recovery is complete              apoorvaj@uber.com
* 2018-07-31    Fix resource usage map panic for older tasks                                           adityacb@uber.com
* 2018-07-31    Add update pause in cli                                                                zhixin@uber.com

0.7.3
------------------
* 2018-07-30    Add tests for hostmgr/task                                                             varung@uber.com
* 2018-07-30    Enqueue initialized tasks being updated into goal state engine                         zhixin@uber.com
* 2018-07-30    Reenqueue update if any task updated in UpdateRun is KILLED                            zhixin@uber.com
* 2018-07-30    Add tests for peloton/placement                                                        varung@uber.com
* 2018-07-30    Stop asyncEventProcessor on statusUpdate stop                                          amitbose@uber.com
* 2018-07-28    Add test cover for jobmgr/task/event/statechanges                                      chunyang.shen@uber.com
* 2018-07-28    Add test cover for  jobmgr/logmanager                                                  chunyang.shen@uber.com
* 2018-07-28    Remove MySQL as Storage for interim                                                    varung@uber.com
* 2018-07-27    Add tests for placement/tasks                                                          varung@uber.com
* 2018-07-27    Add tests for placement/models                                                         varung@uber.com
* 2018-07-27    Merge instance config when update config                                               zhixin@uber.com
* 2018-07-27    Add tests for placement/offers                                                         varung@uber.com
* 2018-07-26    Change gocql version in glide.yaml                                                     adityacb@uber.com
* 2018-07-26    Add host related CLI commands                                                          sachins@uber.com
* 2018-07-26    Implement update with batch size                                                       zhixin@uber.com
* 2018-07-26    Fix potential deadlock case in `AggregatedChildrenReservations`                        avyas@uber.com
* 2018-07-26    Implementing Host Manager's ReserveHosts and GetCompletedReservations api's in placement engine mabansal@uber.com
* 2018-07-26    Add test cover for jobmgr/task/deadline                                                chunyang.shen@uber.com
* 2018-07-26    Add test cover for jobmgr/goalstate                                                    chunyang.shen@uber.com
* 2018-07-25    Add Drainer to reschedule tasks placed on draining hosts                               sachins@uber.com
* 2018-07-25    Add Drainer to reschedule tasks placed on draining hosts                               sachins@uber.com
* 2018-07-25    Handle resource usage calculation during jobmgr recovery                               adityacb@uber.com
* 2018-07-25    Task and job return yarpc not found error in store                                     zhixin@uber.com
* 2018-07-24    Fix the deadlock in task state machine and tracker                                     avyas@uber.com
* 2018-07-24    Modify Start() and Stop() to use common/lifecycle                                      sachins@uber.com
* 2018-07-24    Fixing errors, consistent locking in code cleanup in placement engine, resmgr and Hostmanager mabansal@uber.com
* 2018-07-24    Fix building Peloton-CLI debian package                                                chunyang.shen@uber.com
* 2018-07-24    Persist the health check result into the task runtime                                  chunyang.shen@uber.com
* 2018-07-23    Add resource usage stats to job and task runtime info                                  adityacb@uber.com
* 2018-07-23    Revert "Record state transition durations for RMTask"                                  avyas@uber.com
* 2018-07-23    Adding support in placement engine for processing completed reservation and create placements mabansal@uber.com
* 2018-07-20    Fix `Less` function in task sorting by `creation_time`                                 chunyang.shen@uber.com
* 2018-07-20    Add unit tests for jobmgr task util                                                    adityacb@uber.com
* 2018-07-20    Refactor testutil package and add tests for it                                         mabansal@uber.com
* 2018-07-20    Recover Update upon JobMgr restart                                                     zhixin@uber.com
* 2018-07-20    Allow providing resource pool path in update create CLI                                apoorvaj@uber.com
* 2018-07-19    Add support for AbortUpdate API                                                        apoorvaj@uber.com
* 2018-07-19    Move the changelog version increment and validation to the cache                       apoorvaj@uber.com
* 2018-07-19    Fix nil panic when update cache is not found                                           zhixin@uber.com
* 2018-07-19    Implement update.Recover                                                               zhixin@uber.com
* 2018-07-17    Ensure that previous uncompleted job updates are aborted correctly                     apoorvaj@uber.com
* 2018-07-17    Prevent duplicate entries for same Peloton task in Preemption Queue                    sachins@uber.com

0.7.2
	------------------
* 2018-07-16    Change MockJobConfig to MockJobConfigCache                                             zhixin@uber.com
* 2018-07-13    Add update service handler and CLI to interact with it                                 apoorvaj@uber.com
* 2018-07-13    Controller job decides terminal state with controller task state                       zhixin@uber.com
* 2018-07-13    Validate controller task config                                                        zhixin@uber.com
* 2018-07-12    Disable the periodic GetActiveTask call from JobMgr to ResMgr                          chunyang.shen@uber.com
* 2018-07-12    Move retain base secrets code into task config Merge                                   adityacb@uber.com
* 2018-07-11    Script to send a report Email after performance test                                   chunyang.shen@uber.com
* 2018-07-11    Add cache and goal state engine for job updates                                        apoorvaj@uber.com
* 2018-07-11    Adding Reserver in HostManager which can reserve the hosts for specified tasks         mabansal@uber.com
* 2018-07-11    Fix flaky TestPeriodicCalculationWhenStarted                                           avyas@uber.com
* 2018-07-11    Remove Invalid StartTime log                                                           chunyang.shen@uber.com
* 2018-07-11    Quick fix of cache deadlock                                                            zhixin@uber.com
* 2018-07-11    Move secrets related stuff used in common from jobmanager to util                      apoorvaj@uber.com
* 2018-07-10    Enable GetActiveTasks() in job manager through a in-memory DB                          chunyang.shen@uber.com
* 2018-07-10    Increase test coverage for rmtask                                                      avyas@uber.com
* 2018-07-09    Service job cannot be in FAILED/SUCCEED state                                          zhixin@uber.com
* 2018-07-09    Changelog for 0.7.1.3                                                                  adityacb@uber.com
* 2018-07-09    Instance config should retain secrets from defaultconfig                               adityacb@uber.com
* 2018-07-06    Share integration test cases between stateless and batch job                           zhixin@uber.com
* 2018-07-05    [API] Add resourceUsage map to RuntimeInfo proto                                       adityacb@uber.com
* 2018-07-05    Goal state engine handles job update                                                   zhixin@uber.com
* 2018-07-05    JobMgr Read API stops refilling cache                                                  zhixin@uber.com
* 2018-07-05    Add DB read and write operations for job updates                                       apoorvaj@uber.com
* 2018-07-05    Remove unused cached.UnknownVersion                                                    apoorvaj@uber.com
* 2018-07-05    Add support for upgrading a task in the task goal state engine                         apoorvaj@uber.com
* 2018-07-05    Clean up jobmgr code                                                                   zhixin@uber.com
* 2018-07-03    Replace UpdateTasks with PatchTasks                                                    zhixin@uber.com
* 2018-07-03    Add task actions for stateless jobs                                                    zhixin@uber.com
* 2018-07-03    Add changelog for 0.7.1.2 release                                                      adityacb@uber.com
* 2018-07-02    Don't enforce instance config to use mesos containerizer for secrets                   adityacb@uber.com
* 2018-07-02    Stateless jobs do not call taskStore.GetTaskEvents                                     zhixin@uber.com
* 2018-07-02    Set goalstate to PREEMPTING for tasks to be preempted                                  zhixin@uber.com
* 2018-06-28    Fix peloton-client library in vcluster                                                 chunyang.shen@uber.com
* 2018-06-28    [API] Fix APIs for supporting job updates                                              apoorvaj@uber.com
* 2018-06-28    Define RuntimeDiff to replace map[string]interface{} in runtime update                 zhixin@uber.com
* 2018-06-28    Add set API for pod events.                                                            varung@uber.com
* 2018-06-28    Handle job runtime update when more instances running than expected                    zhixin@uber.com
* 2018-06-28    Remove GetJobConfig from GetTasksByQuerySpec                                           chunyang.shen@uber.com
* 2018-06-28    Base64 decode data before launching task                                               adityacb@uber.com
* 2018-06-27    Add incremental run id                                                                 varung@uber.com
* 2018-06-27    Clean job upon create failure only if job does not exist before                        zhixin@uber.com
* 2018-06-27    Add stateless PE to deploy script                                                      zhixin@uber.com
* 2018-06-26    Adding Test Cases for Tree as well remove duplicate code                               mabansal@uber.com
* 2018-06-25    Use PatchTasks in place of UpdateTasks for simple cases                                zhixin@uber.com
* 2018-06-25    Add stateless PE to pcluster                                                           zhixin@uber.com
* 2018-06-25    set TaskType to STATELESS for stateless service                                        zhixin@uber.com
* 2018-06-25    Revert "Add stateless PE to pcluster"                                                  zhixin@uber.com
* 2018-06-25    Add stateless PE to pcluster                                                           zhixin@uber.com
* 2018-06-25    Refactoring respool handler , add more tests as well as remove duplicate code          mabansal@uber.com
* 2018-06-25    Add task.RuntimeInfo as blob, and update runID as bigint datatype                      varung@uber.com
* 2018-06-25    use SLA instead of Sla                                                                 zhixin@uber.com
* 2018-06-21    Fix TaskList crash when job is not in the cache                                        chunyang.shen@uber.com
* 2018-06-21    Ignore scarce_resource_tpyes if empty                                                  varung@uber.com
* 2018-06-21    Adding Interface for MultiLevel List as well add tests in queue package                mabansal@uber.com
* 2018-06-21    Adding tests for scalar package as well as remove duplicate code from tests            mabansal@uber.com
* 2018-06-21    set env variable ENABLE_SECRETS by reading from cluster config                         adityacb@uber.com
* 2018-06-21    Use ReplaceTasks in place of UpdateTasks with UpdateCacheOnly                          zhixin@uber.com
* 2018-06-21    Implement PatchTasks and ReplaceTasks                                                  zhixin@uber.com
* 2018-06-21    Implement PatchRuntime and ReplaceRuntime                                              zhixin@uber.com
* 2018-06-21    Fix integ and perf tests to use 0.7.1 py client                                        adityacb@uber.com
* 2018-06-20    Add changelog for 0.7.1.1                                                              rcharles@uber.com
* 2018-06-20    Remove duplication from test code as well as separating test case.                     mabansal@uber.com
* 2018-06-18    Change datatype from timestamp to timeuuid                                             varung@uber.com
* 2018-06-18    Add scarce_resource_types to prevent CPU only task to launch on GPU machines           varung@uber.com
* 2018-06-18    Set Mesos Task Labels for JobID, InstanceID, TaskID                                    rcharles@uber.com
* 2018-06-18    Changing resource manager api from MarkTasksLaunched to UpdateTasksState for updating any state into Resourcemanager mabansal@uber.com
* 2018-06-16    Improve unit test coverage for jobsvc                                                  adityacb@uber.com
* 2018-06-15    Add task events table                                                                  varung@uber.com
* 2018-06-15    Add unit tests for async processor                                                     zhixin@uber.com
* 2018-06-14    Reject offers if Unavailability set after maintainence start time                      varung@uber.com
* 2018-06-14    Restrict Max Retries on Task Failures.                                                 varung@uber.com

0.7.1.3
------------------
* 2018-07-09    Instance config should retain secrets from defaultconfig                               adityacb@uber.com
* 2018-07-09    [API] Add resourceUsage map to RuntimeInfo proto                                       adityacb@uber.com

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
