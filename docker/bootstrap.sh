#!/bin/bash

echo 'This script is being deprecated, run "./tools/minicluster/main.py setup" instead'

pushd $(dirname $0)
source config
source mesos_config/config
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

echo "clean up existing containers before bootstrapping the environment"
./cleanup.sh

sleep 10

set -euxo pipefail

# fetch hostIp, this is not required for linux if container is launched with host network, but missing it will break mesos
# connection to zk in container with bridged network on Mac osx. On Mac, bridged network is required for mesos containers
# otherwise DNS service won't be able to resolve IP for containers.
if [ "$(uname)" == "Darwin" ]; then
   hostIp=$(ipconfig getifaddr en0)
   echo 'running on Mac laptop ip '${hostIp}
else
   hostIp=$(ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}')
   echo 'running on Linux dev server ip '${hostIp}
fi

# run zk
echo "run zk container"
$docker_cmd run -d --name $ZK_CONTAINER -p $LOCAL_ZK_PORT:$DEFAULT_ZK_PORT \
  -v $(pwd)/scripts:/scripts \
  netflixoss/exhibitor:$ZK_EXHIBITOR_VERSION

sleep 10

# set up zk nodes
$docker_cmd exec $ZK_CONTAINER /scripts/setup_zk.sh

# run master
echo "run mesos master container"
$docker_cmd run -d --name $MESOS_MASTER_CONTAINER --privileged \
  -e MESOS_LOG_DIR=/var/log/mesos \
  -e MESOS_PORT=$MASTER_PORT \
  -e MESOS_ZK=zk://$hostIp:$LOCAL_ZK_PORT/mesos \
  -e MESOS_QUORUM=$QUORUM \
  -e MESOS_REGISTRY=$REGISTRY \
  -p $MASTER_PORT:$MASTER_PORT \
  -v "$(pwd)/mesos_config/etc_mesos-master:/etc/mesos-master" \
  -v $(pwd)/scripts:/scripts \
  --entrypoint /bin/bash \
  mesosphere/mesos-master:$MESOS_VERSION \
  /scripts/run_mesos_master.sh

# run mesos slaves
for ((i=0; i<$NUM_AGENTS; i++)); do
   echo "run mesos slave container"$i
   container_name=$MESOS_AGENT_CONTAINER$i
   agent_port=$(($AGENT_PORT + $i))
   $docker_cmd run -d --name $container_name --privileged \
     -e MESOS_LOG_DIR=/var/log/mesos \
     -e MESOS_PORT=$agent_port \
     -e MESOS_MASTER=zk://$hostIp:$LOCAL_ZK_PORT/mesos \
     -e MESOS_SWITCH_USER=$SWITCH_USER \
     -e MESOS_CONTAINERIZERS=$CONTAINERS \
     -e MESOS_LOG_DIR=$LOG_DIR \
     -e MESOS_ISOLATION=$ISOLATION \
     -e MESOS_IMAGE_PROVIDERS=$IMAGE_PROVIDERS \
     -e MESOS_IMAGE_PROVISIONER_BACKEND=$IMAGE_PROVISIONER_BACKEND \
     -e MESOS_APPC_STORE_DIR=$APPC_STORE_DIR \
     -e MESOS_WORK_DIR=$WORK_DIR \
     -e MESOS_RESOURCES="$RESOURCES" \
     -e MESOS_MODULES=$MODULES \
     -e MESOS_RESOURCE_ESTIMATOR=$RESOURCE_ESTIMATOR \
     -e MESOS_OVERSUBSCRIBED_RESOURCES_INTERVAL=$OVERSUBSCRIBED_RESOURCES_INTERVAL \
     -e MESOS_QOS_CONTROLLER=$QOS_CONTROLLER \
     -e MESOS_QOS_CORRECTION_INTERVAL_MIN=$QOS_CORRECTION_INTERVAL_MIN \
     -p $agent_port:$agent_port \
     -v /var/run/docker.sock:/var/run/docker.sock \
     -v "$(pwd)/mesos_config/etc_mesos-slave:/etc/mesos-slave" \
     -v $(pwd)/scripts:/scripts \
     --entrypoint /bin/bash \
     mesosphere/mesos-slave:$MESOS_VERSION \
     /scripts/run_mesos_slave.sh
done

echo "All containers are running now."
