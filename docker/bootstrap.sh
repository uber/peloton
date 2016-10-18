#!/bin/bash

source config
source mesos_config/config

echo "clean up existing containers before bootstrapping the environment"
./cleanup.sh

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
sudo docker run -d --name $ZK_CONTAINER -p $LOCAL_ZK_PORT:$DEFAULT_ZK_PORT netflixoss/exhibitor:$ZK_EXHIBITOR_VERSION

# run master
echo "run mesos master container"
sudo docker run -d --name $MESOS_MASTER_CONTAINER --privileged \
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

# run slave
# TODO: run multiple slaves
echo "run mesos slave container"
sudo docker run -d --name $MESOS_AGENT_CONTAINER --privileged \
  -e MESOS_PORT=$AGENT_PORT \
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
  -p $AGENT_PORT:$AGENT_PORT \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)/mesos_config/etc_mesos-slave:/etc/mesos-slave" \
  -v $(pwd)/scripts:/scripts \
  --entrypoint /bin/bash \
  mesosphere/mesos-slave:$MESOS_VERSION \
  /scripts/run_mesos_slave.sh

# run mysql
echo "run mysql container"
sudo docker run --name $MYSQL_CONTAINER -p $LOCAL_MYSQL_PORT:$DEFAULT_MYSQL_PORT -d \
   -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
   -e MYSQL_DATABASE=$MYSQL_DATABASE \
   -e MYSQL_USER=$MYSQL_USER \
   -e MYSQL_PASSWORD=$MYSQL_PASSWORD \
   mysql/mysql-server:$MYSQL_VERSION

echo "All containers are running now."