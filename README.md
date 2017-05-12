The Peloton Unified Resource Scheduler
=======================================

Peloton is a Unified Resource Scheduler built on top of Mesos to
manage compute resources more efficiently for diverse workloads such
as batch, stateless, stateful and daemon jobs. Peloton provides
hierarchical max-min fairness guarantees for different
organizations. It has a few unique design priciples that
differentiates itself from other Mesos schedulers:

1. Scalable and high-avaiable architecture. Unlike other
active-standby schedulers such as Aurora and Marathon, Peloton uses a
scale-out and all-active achitecture where all Peleoton job managers
are able to handle read and write requests concurrently.

2. Support different types of workloads such as batch, stateless,
stateful and daemon jobs. All those workloads can be colocated on a
shared cluster so that we can improve the overall cluster utilization
while provide resource isolation for different workloads.

3. Support hierachical resource pools for elastic resource sharing
among organizations in a shared cluster. The resource pools in Peloton
provide hierarchical max-min fairness guarantees for organizations.

4. Support GPU scheduling for a cluster with mixed GPU and non-GPU
hosts. Any GPU jobs such as TensorFlow or Caffe will be scheduled and
launched to hosts with corresponding GPU resources.

5. Support UNS based service discovery and routing via
Mesos-UNS-Bridge so that the Peloton tasks can be discovered via UNS
or routed to via routing agents like HaProxy or Muttley.

6. Support persistent or immutable volumes for a job. For example,
uConfig or translations data can be mounted into a job instance using
a remote volume referenced by a immutable URL to a
udeploy-replicator. Peloton will use the same job upgrade workflow to
upgrade an uConfig version by simplying changing the URL of a remote
volume.

[Project Page] (http://t.uber.com/peloton)
[Runbook](https://code.uberinternal.com/w/runbooks/peloton/)

## Install

Installations of protoc/proto/protoc-gen-go are required, run
bootstrap.sh once so all build dependencies will be installed.  Want
to build debian package or docker image ? Follow packaging/README.md
```
$ cd $GOPATH

$ mkdir -p src/code.uber.internal/infra/

$ git clone gitolite@code.uber.internal:infra/peloton src/code.uber.internal/infra/peloton

$ cd $GOPATH/src/code.uber.internal/infra/peloton

$ ./bootstrap.sh

$ glide install

$ make devtools

$ make
```


## Run Peloton apps in containers
Build docker image:
```
$ IMAGE=uber/peloton make docker
```

Launch all dependencies and peloton apps in containers:
```
$ PELOTON=app make pcluster
```

## Test Peloton

Create resource pool by providing the path(respool is required for job creation):
```
$ bin/peloton respool create /DefaultResPool example/default_respool.yaml
Resource Pool d214ed86-1cf5-4e39-a0bb-08399ab1dee0 created at /DefaultResPool
```

Create job:

create job requires the resource pool path(where the job is to be subitted) and the job config
```
$ bin/peloton job create /DefaultResPool example/testjob.yaml
Job 91b1b8e5-2ba8-11e7-bc23-0242ac11000d created
```

Get tasks:
```
$ bin/peloton task list <job ID>
Instance|        Job|  CPU Limit|  Mem Limit|  Disk Limit|      State|  GoalState|  Started At|                                                                       Task ID|  Host|  Message|  Reason|
         0|  instance0|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-0-91b7fa56-2ba8-11e7-bc23-0242ac11000d|      |         |        |
         1|  instance1|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-1-91b82b47-2ba8-11e7-bc23-0242ac11000d|      |         |        |
         2|  instance2|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-2-91b837b9-2ba8-11e7-bc23
```


## Curl into Peloton endpoint:

curl -X POST  \
     -H 'content-type: application/json'  \
     -H 'Rpc-Procedure: JobManager.Get'   \
     -H 'Rpc-Service: peloton-jobmgr'     \
     -H 'Rpc-Caller: peloton-client'      \
     -H 'Context-TTL-MS: 1000'            \
     -H 'Rpc-Encoding: json'              \
     --data '{"id": {"value": "myjob12345"}}' 	\
    localhost:5292/api/v1


## Run unit tests
```
$ make test
```

## Run integration tests

Against local pcluster:
```
$ make integ-test
```

Against a real cluster:
irn1 (ATG VPN connection required) :
```
$ CLUSTER=<name of the cluster, i.e. peloton-devel01> make integ-test
```
dca1/sjc1 (run this from any compute host in the target dc):
```
$ CLUSTER=<name of the cluster, i.e. dca1-devel01> make integ-test
```

## Run peloton from docker container

### Build

```
$ make docker
...
Built uber/peloton:51f1c4f
```

If you want to build an image with a different name: `IMAGE=foo/bar:baz make docker`

### Run

The docker container takes a few environment variables to configure how it will run. Each peloton
app is launchable by setting `APP=$name` in the environment. For example, to run the
peloton-master, execute:

```
$ docker run --rm --name peloton -it -p 5289:5289 -e APP=master -e ENVIRONMENT=development peloton
```

Configurations are stored in `/etc/peloton/$APP/`, and by default we will pass the following
arguments: `-c "/etc/peloton/${APP}/base.yaml" -c "/etc/peloton/${APP}/${ENVIRONMENT}.yaml"`

NOTE: you need to make sure the container has access to all the dependencies it needs, like mesos-master,
zookeeper, mysql, cassandra, etc. Check your configs!

#### Master with pcluster

```
$ make pcluster
$ docker run --rm --name peloton -it -e APP=master -e ENVIRONMENT=development --link peloton-mesos-master:mesos-master --link peloton-zk:zookeeper --link peloton-mysql:mysql --link peloton-cassandra:cassandra peloton
```

#### Client

Launching the client is similar (replace `-m` argument with whereever your peloton-master runs:

```
$ docker run --rm -it --link peloton:peloton -e APP=client peloton job -m http://peloton:5289/ create test1 test/testjob.yaml
```



## Packaging

Build debs for supported distributions. Output will be placed into `./debs`. You can specify
the DISTRIBUTION by passing `DISTRIBUTION=jessie` (jessie and trusty are supported). Defaults
to `all`.

```
$ make debs
```

## Tagging a new release

Releases are managed by git tags, using semantic versioning. To tag a new release:

Check the current version:
```
$ make version
0.1.0-abcdef
```

Make sure you are on master, and have the proper sha at HEAD you want to tag. Then,
increment the version and tag, then push tags:

```
$ git tag -a 0.2.0
...
$ git push origin --tags
```

## Pushing docker containers

`make docker-push` will build docker containers, and push them to both ATG and
SJC1 registries. You can push to only one DC with `DC=atg` or `DC=sjc1`. You can
override the image to push with `IMAGE=foo/bar:baz`

To build and deploy docker containers everywhere:

```
make docker docker-push
```

## Debug Peloton Apps in Docker Container


1. Find docker container process ID:
sudo docker inspect -f {{.State.Pid}} <DOCKER_IMAGE_ID>

2. Run a bash shell in the container:
nsenter -t <PID> -m -p bash

3. Setup source code directory symlink:
mkdir -p /workspace/src/code.uber.internal/infra/
ln -s /peloton-install /workspace/src/$(make project-name)

4. Start the gdb in the bash shell:
gdb peloton-install/bin/peloton-[hostmgr|jobmgr|resmgr|placement] <PID>

5. Happy debugging ;-)


## Pressure test the cassandra store

We have a tool for pressure testing the cassandra store, which is based on the storage.TaskStore interface.

1. Build the cassandra store tool:
make db-pressure

2. Run test against a cassandra store. For example

bin/dbpressure -s peloton_pressure -t 1000 -w 200 -h ms-3162c292.pit-irn-1.uberatc.net -c ONE

Also need to make sure to have the schema migration files under storage/cassandra/migrations

Flags:
      --help             Show context-sensitive help (also try --help-long and --help-man).
  -h, --cassandra-hosts=CASSANDRA-HOSTS
                         Cassandra hosts
  -c, --consistency="LOCAL_QUORUM"
                         data consistency
  -w, --workers=WORKERS  number of workers
  -s, --store=STORE      store
  -t, --batch=BATCH      task batch size per worker
  
After running the load into C*, one can check the C* dashboard, for example 
https://graphite.uberinternal.com/grafana2/dashboard/db/cassandra-mesos-irn