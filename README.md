The Peloton Meta-Framework
===========================


Peloton is Uber's meta-framework for managing, scheduling and
upgrading jobs on Mesos clusters. It has a few unique design priciples
that differentiates itself from other Mesos meta-frameworks:

1. Scalable and high-avaiable multi-master architecture. Unlike other
active-standby frameworks such as Aurora and Marathon, Peloton uses a
multi-master and all-active achitecture where all Peleoton instances
are able to handle both read and write requests concurrently. There is
no single-point of failure or fail-over from standby to active in the
system.

2. Support guranteed rollback when a job upgrade fails. Peloton uses
the latest Mesos resource reservation primitive to upgrade the tasks
on the same nodes as far as possible based on current resource
utilization and scheduling decision. It also reserves the resources on
existing nodes until the upgrade is successful. Otherwise, it will
quickly rollback to previous job configuration on previous nodes
without other dependencies like docker registry.

3. Support persistent or remote volumes for a job. For example,
uConfig or translations data can be mounted into a job instance using
a remote volume referenced by a immutable URL to a
udeploy-replicator. Peloton will use the same job upgrade workflow to
upgrade an uConfig version by simplying changing the URL of a remote
volume.


## Install
Installations of protoc/proto/protoc-gen-go are required

cd $GOPATH
mkdir -p src/code.uber.internal/infra/
git clone gitolite@code.uber.internal:infra/peloton/ src/code.uber.internal/infra/peloton
cd src/code.uber.internal/infra/peloton
glide install
make

## Run Peloton master

UBER_CONFIG_DIR=config/master bin/peloton-master


## Test Peloton master

curl -X POST  \
     -H 'content-type: application/json'  \
     -H 'Rpc-Procedure: JobManager.Get'   \
     -H 'Rpc-Service: peloton-master'     \
     -H 'Rpc-Caller: peloton-client'      \
     -H 'Context-TTL-MS: 1000'            \
     -H 'Rpc-Encoding: json'              \
     --data '{"id": {"value": "myjob12345"}}' 	\
    localhost:5289
