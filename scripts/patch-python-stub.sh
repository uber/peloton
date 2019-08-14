#!/bin/bash


# patch  Python generated grpc code
for file in $( find $1/peloton/api/v0 -name "*_pb2.py"  \
	&& find $1/peloton/api/v0 -name "*_pb2_grpc.py")
do
	echo "patching service name in RPC code in $file"
    sed "s/peloton\.api\.v0\.job\.JobManager/peloton\.api\.job\.JobManager/" $file > $file.tmp && mv $file.tmp $file
    sed "s/peloton\.api\.v0\.task\.TaskManager/peloton\.api\.task\.TaskManager/" $file > $file.tmp && mv $file.tmp $file
    sed "s/peloton\.api\.v0\.respool\.ResourceManager/peloton\.api\.respool\.ResourceManager/" $file > $file.tmp && mv $file.tmp $file
done


for file in $(find $1 -name '*.py' -type f)
do
    echo "patching imports for $file"

    for pattern in "s/^from peloton/from peloton_client.pbgen.peloton/" \
                   "s/^import peloton./import peloton_client.pbgen.peloton./" \
                   "s/^from mesos.v1/from peloton_client.pbgen.mesos.v1/" \
                   "s/^import mesos.v1/import peloton_client.pbgen.mesos.v1/" \
                   "s/^from google.protobuf/from peloton_client.google.protobuf/"
    do
        sed "$pattern" $file > $file.tmp && mv $file.tmp $file
    done
done