#!/bin/bash
set -e
pushd $(dirname $0)
source config
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

$docker_cmd stop $ZK_CONTAINER $MESOS_MASTER_CONTAINER

# stop mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   $docker_cmd stop $container_name
done

