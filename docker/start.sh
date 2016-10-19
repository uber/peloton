#!/bin/bash

source config

sudo docker start $ZK_CONTAINER
sudo docker start $MESOS_MASTER_CONTAINER
sudo docker start $MYSQL_CONTAINER

# start mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   sudo docker start $container_name
done