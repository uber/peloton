#!/bin/bash

source config

# it's okay if stop/rm command fail when there is no peloton related conainer
./stop.sh
sudo docker rm -f $ZK_CONTAINER $MESOS_MASTER_CONTAINER $MYSQL_CONTAINER

# rm mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   sudo docker rm -f $container_name
done
