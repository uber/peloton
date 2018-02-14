# Changelog for Peloton

0.7.0 (unreleased)
------------------

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

