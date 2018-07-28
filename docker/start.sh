#!/bin/bash
set -e
pushd $(dirname $0)
source config
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

$docker_cmd start $ZK_CONTAINER
$docker_cmd start $MESOS_MASTER_CONTAINER

# start mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   $docker_cmd start $container_name
done
