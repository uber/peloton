This directory contains integration tests that involve component failures
("failure testing"). The tests can be run against a Peloton cluster that has
been set up in a few different ways:
- local developer setup (minicluster)
- virtualized Peloton cluster (vcluster)

Tests are written in Python and use pytest as the test runner. See file
test_resmgr_failure.py as an example.

Usage
-----
* Testing against minicluster
 1. Build a local docker image for Peloton
    IMAGE=uber/peloton make docker
 2. Setup minicluster and execute the tests
    $PELOTON_HOME/tests/run-failure-tests.sh minicluster
 3. If all tests pass, the created minicluster will be cleaned up.

* Testing against vcluster
  1. Ensure the following environment variables are set:
     a. VCLUSTER_ZOOKEEPER is set to the location of Zookeeper where underlying    Peloton is running
     b. VCLUSTER_PELOTON_IMAGE is set to the Docker image of Peloton that
        you want to test against (see step 2).
  2. Build a docker image for Peloton and push it to remote registries
     make docker
     make docker-push
  3. Setup vcluster and execute the tests
     $PELOTON_HOME/tests/run-failure-tests.sh vcluster
  4. If all tests pass, the created vcluster will be cleaned up.
  5. To override the defaults used to create the vcluster, use the following
     environment variables
     VCLUSTER_CONFIG_FILE - default is $PELOTON_HOME/tools/vcluster/config/default-small.yaml
     VCLUSTER_RESPOOL - default is /DefaultResPool
     VCLUSTER_LABEL - default is ${USER}_failure_test
     VCLUSTER_AGENT_NUM - default is 3

* Tips/Notes about writing failure tests:
      1. Terminate all jobs and deallocate the resource pools at the end of any failure test besides aurorabridge failure test.
      2. Delete all stateless jobs at the end of each aurorabridge failure test but deallocate resource pools only after all aurorabridge failure tests are completed.
      3. The resource pool in aurorabrideg is cached in aurorabridge, deallocating the resource pools at the end of each aurorabridge failure test may cause reusing the cached resource pool while the resource pool is already deallocated.

* Advanced usage
  1. To avoid cleaning up the Peloton cluster at the end of the run, provide
     option --skip-cleanup
  2. Similarly to reuse a previously deployed cluster, provide --skip-setup.
     These two options are useful during test development because it allows you
     to avoid waiting for the Peloton cluster to be created/destroyed.
  3. Options provided on the command-line after the driver-name are passed on to
     pytest. This can be used to run only a subset of tests, for example
     $PELOTON_HOME/tests/run-failure-tests.sh minicluster tests/failure/test_resmgr_failure.py::TestResMgrFailure::test_resmgr_restart_job_succeeds
     will run only the test TestResMgrFailure.test_resmgr_restart_job_succeeds in file test_resmgr_failure.py
