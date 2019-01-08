#!/bin/bash

cqlsh -e "create keyspace IF NOT EXISTS peloton_test with replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
