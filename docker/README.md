This folder includes scripts to locally run and manage a personal dev env cluster in containers.
Dependencies:
1) docker engine: expected version >=1.12.1
2) docker-py: run "pip install docker-py" or "$PELOTON_HOME/bootstrap.sh" to install
Update 'num_agents' in config.yaml to change number of mesos slaves, by default 3 agents.

Usage:

To bootstrap the local dev env cluster (clean up existing containers and launch brand new ones) :

$PELOTON_HOME/docker/pcluster.py setup

To destroy the local dev env cluster (clean up all existing containers):

$PELOTON_HOME/docker/pcluster.py teardown
