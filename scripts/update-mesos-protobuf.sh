#!/bin/bash

if [ $# -ne 1 ]
then
    echo "Usage:"
    echo "$0 MESOS_HOME"
    exit -1
fi

MESOS_HOME=$1
VERSION="v1"

pushd . > /dev/null
cd ${MESOS_HOME}/include/mesos/
PB_FILES=$(find ${VERSION}  -name '*.proto')
popd > /dev/null

for file in ${PB_FILES}
do
    dst_file="protobuf/mesos/${file}"
    dst_dir=$(dirname ${dst_file})
    if [ ! -d ${dst_dir} ]
    then
        mkdir -p ${dst_dir}
    fi
    echo "Copying ${MESOS_HOME}/include/mesos/${file} ==> ${dst_file}"
    cp ${MESOS_HOME}/include/mesos/${file} ${dst_file}
done
