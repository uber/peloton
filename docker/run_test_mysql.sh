#!/bin/bash

# This script is called by 'make test'

pushd $(dirname $0)
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

# run mysql container for local tests, this script should be called from peloton root dir

source config

$docker_cmd rm -f $MYSQL_TEST_CONTAINER

$docker_cmd run --name $MYSQL_TEST_CONTAINER -p $TEST_MYSQL_PORT:$DEFAULT_MYSQL_PORT -d \
   -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
   -e MYSQL_DATABASE=$MYSQL_DATABASE \
   -e MYSQL_USER=$MYSQL_USER \
   -e MYSQL_PASSWORD=$MYSQL_PASSWORD \
   mysql/mysql-server:$MYSQL_VERSION
