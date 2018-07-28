#!/bin/bash
set -e
pushd $(dirname $0)
source config
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

# it's okay if stop/rm command fail when there is no peloton related conainer
./stop.sh
$docker_cmd rm -f $ZK_CONTAINER $MESOS_MASTER_CONTAINER

# rm mesos slave containers
for ((i=0; i<$NUM_AGENTS; i++)); do
   container_name=$MESOS_AGENT_CONTAINER$i
   $docker_cmd rm -f $container_name
done
