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

max_wait_cycles=20
i=0
until nc -z localhost $CASSANDRA_CQL_PORT ; do
  echo "waiting for cassandra container to begin listening..."
  sleep 0.5
  let i+=1
  if [[ $i -ge $max_wait_cycles ]] ; then
    echo "cassandra container was not listening after $i test cycles, aborting"
    exit 1
  fi
done
