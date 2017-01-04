#!/bin/bash

# This script is called by 'make test'

pushd $(dirname $0)
[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

# run cassandra container for local tests, this script should be called from peloton root dir

source config

$docker_cmd rm -f $CASSANDRA_TEST_CONTAINER

CONTAINER_ID=$($docker_cmd run --name $CASSANDRA_TEST_CONTAINER -d \
  -p $CASSANDRA_CQL_PORT:$CASSANDRA_CQL_PORT \
  -p $CASSANDRA_THRIFT_PORT:$CASSANDRA_THRIFT_PORT \
   cassandra:$CASSANDRA_VERSION)

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

# Create test keyspace in C*
# Need to retry connecting to C* container -- on mac it can take > 10 seconds even if with the nc -z retry logic above
# TODO: investigate and see if this is a docker issue

RESULT=$($docker_cmd exec -t $CONTAINER_ID cqlsh -e "create keyspace IF NOT EXISTS $CASSANDRA_TEST_DB with replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
CONN_ERR="Connection error"
i=0
echo "Attempt connect C* 0 $RESULT"
while [[ $RESULT == $CONN_ERR* ]] && [[ $i -lt 40 ]]
do
   sleep 1
   let i+=1
   RESULT=$($docker_cmd exec -t $CONTAINER_ID cqlsh -e "create keyspace IF NOT EXISTS $CASSANDRA_TEST_DB with replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
   echo "Attempt connect C* $i $RESULT"
   if [ "$RESULT" == "" ];then
      echo "C* DB $CASSANDRA_TEST_DB is created"
      exit
   fi
done

echo "Cannot create C* DB $CASSANDRA_TEST_DB"
exit 1
