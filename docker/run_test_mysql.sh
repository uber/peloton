#!/bin/bash

# run mysql container for local tests, this script should be called from peloton root dir

source docker/config

sudo docker rm -f $MYSQL_TEST_CONTAINER

sudo docker run --name $MYSQL_TEST_CONTAINER -p $TEST_MYSQL_PORT:$DEFAULT_MYSQL_PORT -d \
   -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
   -e MYSQL_DATABASE=$MYSQL_DATABASE \
   -e MYSQL_USER=$MYSQL_USER \
   -e MYSQL_PASSWORD=$MYSQL_PASSWORD \
   mysql/mysql-server:$MYSQL_VERSION
