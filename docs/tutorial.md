# Peloton Tutorial

This tutorial will walk you step-by-step through using Peloton:

* Start a local minicluster (local cluster with peloton running)
* Install Peloton CLI
* HelloWorld Example
    - Create a Resource Pool
    - Create a HelloWorld job
    - Check job status
    - List all job instances
    - Check the logs for a particular instance
* Elastic Resource Pool Example (coming soon)


## Start a minicluster
A `minicluster` can be started locally on a development machine. It
consists of Peloton components as well as dependencies like Zookeeper,
Cassandra, Mesos master and agents. A minicluster launches all
components via docker engine which has to be installed first. Below
are the steps to start a minicluster:

- Install docker engine (>=1.12.1) per [instruction here](https://docs.docker.com/v17.12/install/)
- Install `docker-py` python package.
```
$ pip install docker-py
```
- Pull the latest Peloton image from [docker-hub](https://cloud.docker.com/u/uber/repository/docker/uber/peloton).
```
$ docker pull uber/peloton
```
- Start minicluster locally.

```
$ PELOTON=app make minicluster
```

## Install Peloton CLI

Peloton CLI package will be available, in the mean time please build
peloton locally. Please see the [Peloton Developer
Guide](developer-guide.md) for how to setup environment and build
Peloton CLI binary.

```
$ make
```
Peloton CLI is available on
```
$ {$home}/bin/peloton
```

## Hello World Example
### Create a resource pool
A resource pool is required before starting any jobs in a Peloton
cluster. Each resource pool specifies the reservation, limit and
shares of the resources for an organization or team. Here is an
example spec for a resource pool which reserves 12 CPU cores, 4GB
memory and 2 GPUs.

```
name: HelloWorldPool
owningteam: MyTeam
ldapgroups:
- MyGroup
description: "My first resource pool on Peloton"
resources:
- kind: cpu
  reservation: 12
  limit: 24
- kind: memory
  reservation: 4096
  limit: 8192
  share: 1
- kind: gpu
  reservation: 2
  limit: 4
  share: 1
```

To create a resource pool, specify the path and spec of the resource pool as follows:
```
$ bin/peloton respool create /HelloWorldPool example/helloworld_pool.yaml
Resource Pool fa651fc9-086d-4e8f-a823-d6bf6f144481 created at /HelloWorldPool
```

### Create a job
A job consists of multiple instances in Peloton. A resource pool and a
job spec are required to create a job. The resource pool specifies
where the resources are accounted for the job to run. The job spec
describes the detailed configuration of the job, such as resource
limits, container image, etc. Below is an example job spec with 10
instances. Each job instance echoes a message and then sleeps for 1
minute.

```
name: HelloWorld
owningteam: MyTeam
ldapgroups:
- MyGroup
description: "A Hello World batch job on Peloton"
instancecount: 10
defaultconfig:
  resource:
    cpulimit: 1
    memlimitmb: 1024
    disklimitmb: 1024
    fdlimit: 10
  container:
    type: 1
    docker:
      image: "debian"
      parameters:
        - key: env
          value: MESSAGE=HelloWorld
        - key: env
          value: SLEEP_SECONDS=300
    volumes:
      - containerpath: /tmp
        hostpath: /tmp
        mode: 1
  command:
    shell: true
    value: 'echo $MESSAGE && sleep $SLEEP_SECONDS'

```

To create the example job, specify the resource pool and job spec as follows:

```
$ bin/peloton job create /HelloWorldPool example/helloworld_job.yaml
Job 3a6d6cfe-4b25-4137-af65-61d3070d4ac3 created
```

### Check job status

```
$ bin/peloton job status 3a6d6cfe-4b25-4137-af65-61d3070d4ac3

creationTime: 2019-02-09T02:32:17.3281139Z
desiredStateVersion: "1"
goalState: SUCCEEDED
startTime: 2019-02-09T02:32:24.448294878Z
state: RUNNING
stateVersion: "1"
taskStats:
  DELETED: 0
  FAILED: 0
  INITIALIZED: 0
  KILLED: 0
  KILLING: 0
  LAUNCHED: 0
  LAUNCHING: 0
  LOST: 0
  PENDING: 8
  PLACED: 0
  PLACING: 0
  PREEMPTING: 0
  READY: 0
  RUNNING: 2
  STARTING: 0
  SUCCEEDED: 0
  UNKNOWN: 0
```

###  List job instances
```
$ bin/peloton task list  3a6d6cfe-4b25-4137-af65-61d3070d4ac3
  Instance|  Name|    State|   Healthy|            Start Time|  Run Time|                  Host|                  Message|  Reason|
         0|      |  RUNNING|  DISABLED|  2019-02-09T02:32:27Z|  00:00:11|  peloton-mesos-agent2|                         |        |
         1|      |  RUNNING|  DISABLED|  2019-02-09T02:32:27Z|  00:00:11|  peloton-mesos-agent2|                         |        |
         2|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         3|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         4|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         5|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         6|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         7|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         8|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
         9|      |  PENDING|  DISABLED|                      |          |                      |  Task sent for placement|        |
```

###  Check the logs for a particular instance
The stdout / stderr can be streamed over the commandline. 
Note: the stdout is keep in the Mesos Sandbox which is automatically cleaned when space is running out
```
$ bin/peloton task logs 3a6d6cfe-4b25-4137-af65-61d3070d4ac3 0 --filename="stderr"
HelloWorld
```

## Elastic Resource Pool Example

Coming soon...
