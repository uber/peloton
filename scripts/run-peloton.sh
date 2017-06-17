#!/bin/bash

for app in jobmgr resmgr hostmgr placement
do
    echo -n "Starting peloton-${app} ... "
    bin/peloton-${app} -c config/${app}/base.yaml -c config/${app}/development.yaml &> .log/${app}.log &
    echo "Done (pid: $!)"
done
