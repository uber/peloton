#!/bin/bash

source config

sudo docker start $ZK_CONTAINER
sudo docker start $MESOS_MASTER_CONTAINER
sudo docker start $MESOS_AGENT_CONTAINER
sudo docker start $MYSQL_CONTAINER