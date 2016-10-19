This folder includes scripts to launch all dependencies in different containers on localhost : zk, mesos master, mesos agent, mysql.
Docker installation is required, expected version : 1.12.1. Update 'NUM_AGENTS' in docker/config to change number of mesos slaves,
by default 3 agents.

Usage:

To bootstrap the environment (clean up old containers if exist and launch brand new ones) :

./bootstrap.sh

To stop all containers :

./stop.sh

To start all containers :

./start.sh

To clean up all existing containers :

./cleanup.sh
