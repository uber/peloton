#!/bin/bash

for file in $(find .gen/peloton/api/v0 -name "*.pb.go" -o -name "*.pb.yarpc.go")
do
    sed "s/peloton\.api\.v0\.job\.JobManager/peloton\.api\.job\.JobManager/" $file > $file.tmp && mv $file.tmp $file
    sed "s/peloton\.api\.v0\.task\.TaskManager/peloton\.api\.task\.TaskManager/" $file > $file.tmp && mv $file.tmp $file
    sed "s/peloton\.api\.v0\.respool\.ResourceManager/peloton\.api\.respool\.ResourceManager/" $file > $file.tmp && mv $file.tmp $file
done
