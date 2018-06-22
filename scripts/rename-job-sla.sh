#!/bin/bash

file=.gen/peloton/api/v0/job/job.pb.go
sed 's/Sla /SLA /g' $file | sed 's/Sla$/SLA/g' | sed 's/GetSla/GetSLA/g' > $file.tmp && mv $file.tmp $file
