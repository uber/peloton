#!/bin/bash

source config
source mesos_config/config

echo "clean up existing containers before bootstrapping the environment"
./cleanup.sh

set -euxo pipefail

# run zk
echo "run zk container"
sudo docker run -d --name $ZK_CONTAINER -p $LOCAL_ZK_PORT:$DEFAULT_ZK_PORT netflixoss/exhibitor:$ZK_EXHIBITOR_VERSION

# run master
echo "run mesos master container"
sudo docker run -d --name $MESOS_MASTER_CONTAINER --net=host --privileged \
  -e MESOS_PORT=5050 \
  -e MESOS_ZK=zk://127.0.0.1:$LOCAL_ZK_PORT/mesos \
  -e MESOS_QUORUM=$QUORUM \
  -e MESOS_REGISTRY=$REGISTRY \
  -e MESOS_ROLES=$ROLES \
  -v "$(pwd)/mesos_config/etc_mesos-master:/etc/mesos-master" \
  -v $(pwd)/scripts:/scripts \
  --entrypoint /bin/bash \
  mesosphere/mesos-master:$MESOS_VERSION \
  /scripts/run_mesos_master.sh

# run slave
# TODO: run multiple slaves
echo "run mesos slave container"
sudo docker run -d --name $MESOS_AGENT_CONTAINER --net=host --privileged \
  -e MESOS_PORT=5051 \
  -e MESOS_MASTER=zk://127.0.0.1:$LOCAL_ZK_PORT/mesos \
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