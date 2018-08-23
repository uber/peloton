This directory contains integration tests that involve component failures
("failure testing"). The tests can be run against a Peloton cluster that has
been set up in a few different ways:
- local developer setup (pcluster)
- virtualized Peloton cluster (vcluster)

Tests are written in Python and use pytest as the test runner. See file
test_resmgr_failure.py as an example.

Usage
-----
* Testing against pcluster
 1. Build a local docker image for Peloton
    IMAGE=uber/peloton make docker
 2. Setup pcluster and execute the tests
    $PELOTON_HOME/tests/run-failure-tests.sh pcluster
 3. If all tests pass, the created pcluster will be cleaned up.

* Testing against vcluster
  1. Ensure environment variable GOPATH is set and $GOPATH/bin is in your PATH
  2. Build a docker image for Peloton and push it to remote registries
     make docker
     make docker-push
  3. Setup vcluster and execute the tests
     $PELOTON_HOME/tests/run-failure-tests.sh vcluster
  4. If all tests pass, the created vcluster will be cleaned up.
  5. To override the defaults used to create the vcluster, use the following
     environment variables
     VCLUSTER_CONFIG_FILE - default is $PELOTON_HOME/tools/vcluster/config/default-small.yaml
     VCLUSTER_ZOOKEEPER - default is zookeeper-mesos-preprod01.pit-irn-1.uberatc.net
     VCLUSTER_RESPOOL - default is /DefaultResPool
     VCLUSTER_LABEL - default is ${USER}_failure_test
     VCLUSTER_PELOTON_VERSION - default is inferred using 'git describe'
     VCLUSTER_AGENT_NUM - default is 3


* Advanced usage
  1. To avoid cleaning up the Peloton cluster at the end of the run, provide
     option --skip-cleanup
  2. Similarly to reuse a previously deployed cluster, provide --skip-setup.
     These two options are useful during test development because it allows you
     to avoid waiting for the Peloton cluster to be created/destroyed.
  3. Options provided on the command-line after the driver-name are passed on to
     pytest. This can be used to run only a subset of tests, for example
     $PELOTON_HOME/tests/run-failure-tests.sh pcluster tests/failure/test_resmgr_failure.py::TestResMgrFailure::test_resmgr_restart_job_succeeds
     will run only the test TestResMgrFailure.test_resmgr_restart_job_succeeds in file test_resmgr_failure.py
