#!/bin/bash

# This script is called by 'make test'

pushd $(dirname $0)
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

# run cassandra container for local tests, this script should be called from peloton root dir

source config

$docker_cmd rm -f $CASSANDRA_TEST_CONTAINER

$docker_cmd run --name $CASSANDRA_TEST_CONTAINER -d \
  -p $CASSANDRA_CQL_PORT:$CASSANDRA_CQL_PORT \
  -p $CASSANDRA_THRIFT_PORT:$CASSANDRA_THRIFT_PORT \
   cassandra:$CASSANDRA_VERSION
