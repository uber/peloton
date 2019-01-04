This folder includes scripts to locally run and manage a personal dev env cluster in containers.
Dependencies:
1) docker engine: expected version >=1.12.1
2) docker-py: run "pip install docker-py" or "$PELOTON_HOME/bootstrap.sh" to install
Update 'num_agents' in config.yaml to change number of mesos slaves, by default 3 agents.

Note:
To run peloton containers, please build docker image first by running the following command:
IMAGE=uber/peloton make docker
If you have made changes to some app and want to test it out with other apps, feel free to
specify "--no-<app>" flags with pcluster setup command so it'll be skipped, then you can run
it via command line:
./bin/peloton-<app> -c config/<app>/base.yaml -c config/<app>/development.yaml -d

Alternately, you can use BIND_MOUNTS to mount binaries from your development environment
into the container. This is helpful when you want to setup pcluster with your code
changes without waiting to build a Docker image each time (for example, to run some
integration tests). See Usage section for details.

Usage:

To bootstrap the local dev env cluster w/o peloton:

$PELOTON_HOME/tools/pcluster/pcluster.py setup

To bootstrap the local dev env cluster w/ peloton master:

$PELOTON_HOME/tools/pcluster/pcluster.py setup -m

To bootstrap the local dev env cluster w/ all peloton apps:

$PELOTON_HOME/tools/pcluster/pcluster.py setup -a

To bootstrap the local dev env cluster w/ peloton apps, excluding one or more apps:

$PELOTON_HOME/tools/pcluster/pcluster.py setup -a --no-<app name, i.e jobmgr,resmgr,hostmgr,placement, specify multiple times for multiple apps>

To bootstrap the Peloton only components (no Mesos and ZK) by pointing to an existing ZK:

$PELOTON_HOME/tools/pcluster/pcluster.py setup --no-mesos --zk_url="192.168.33.6:2181" -a

To destroy the local dev env cluster (clean up all existing pcluster related containers):

$PELOTON_HOME/tools/pcluster/pcluster.py teardown

To check peloton app logs:

docker logs -f peloton-<app i.e jobmgr,resmgr,hostmgr,placement,><instance id, i.e. 0>

To mount your local binaries inside the container:

* Cross-compile Peloton for Linux. This step is necessary if you are NOT building on Linux,
such as your MacOS laptop. Place the produced binaries in a different directory
(say `bin-linux`) so that you do not overwrite your MacOS binaries in `bin`

    GOOS=linux GOARCH=amd64 BIN_DIR=bin-linux make

* Set environment variable BIND_MOUNTS to map the binaries directory in the container.
Expected format for this variable is a comma-separated list of items of the form
\<host-path>:\<container-path>

    BIND_MOUNTS=$PWD/bin-linux:/go/src/github.com/uber/peloton/bin \
    $PELOTON_HOME/tools/pcluster/pcluster.py setup -a

  Replace $PWD/bin-linux with $PWD/bin if you are building on Linux.
