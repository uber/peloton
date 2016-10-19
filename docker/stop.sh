#!/bin/bash

source config

sudo docker stop $ZK_CONTAINER $MESOS_MASTER_CONTAINER $MYSQL_CONTAINER

# stop mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   sudo docker stop $container_name
done

