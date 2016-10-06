#!/bin/bash

source config

# it's okay if stop/rm command fail when there is no peloton related conainer
./stop.sh
sudo docker rm -f $ZK_CONTAINER $MESOS_MASTER_CONTAINER $MESOS_AGENT_CONTAINER $MYSQL_CONTAINER
