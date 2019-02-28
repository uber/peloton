#Peloton CLI

Peloton CLI is a command line interface for interacting with Peloton clusters.
This section covers Peloton CLI  usage and provides commands examples.
All Peloton CLI commands start with peloton and is followed by the flag, commands, and arguments.

To communicate with a Peloton cluster, you need to provide the zookeeper
information or the cluster name you want to access.

## Requirement
You need to have a working Peloton CLI binary installed and a Peloton
cluster.

## Usage
To get all the help message for using Peleton Cli and all the 8 Peloton Cli
flags
```
$ ./peloton
```
```
Flags:
  -h, --help                     Show context-sensitive help (also try --help-long and --help-man).
  -j, --json                     print full json responses
  -m, --jobmgr=localhost:5392    name of the jobmgr address to use (grpc) (set $JOBMGR_URL to override)
  -v, --resmgr=localhost:5394    name of the resource manager address to use (grpc) (set $RESMGR_URL to override)
  -u, --hostmgr=localhost:5391   name of the host manager address to use (grpc) (set $HOSTMGR_URL to override)
  -e, --clusterName=CLUSTERNAME  name of the cluster you want to connect to.To use this feature, please create a clusters.json file or clone it from a cluster config repo
                                 into ~/.peloton or /etc/peloton dir
  -z, --zkservers=ZKSERVERS ...  zookeeper servers used for peloton service discovery. Specify multiple times for multiple servers(set $ZK_SERVERS to override with ' ' as
                                 delimiter)
      --zkroot="/peloton"        zookeeper root path for peloton service discovery(set $ZK_ROOT to override)
  -t, --timeout=20s              default RPC timeout (set $TIMEOUT to override)
      --version                  Show application version.
```

To use -e flag feature, users have to create a cluster.json either in  ~/
.peloton or /etc/peloton dir.
Format should like this
```
{
  "clusters":[
    {
      "clusterName":"clusterOne",
      "zkURL":"$zookeeper-url: $portNum"
    },
    {
      "clusterName":"clusterTwo",
      "zkURL":"$zookeeper-url: $portNum"
    }
  ]
}
```

To create a resource pool
```
$./peloton respool create <respool> <config>
$./peloton respool create /DefaultResPool example/default_respool.yaml
```
To view information of all resource pool(s)
```
$./peloton respool dump [<flags>]
$./peloton respool dump -z zookeeperURL
```
To create a peloton job
```
$./peloton job create [<flags>] <respool> <config>
$./peloton job create /DefaultResPool example/testjob.yaml
```
To get a peloton job information including configs and runtime
```
$./peloton job get [<flags>] <job>
$./peloton job get -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76
```

To only get a peloton job run time information
```
$./peloton job status [<flags>] <job>
$./peloton job status -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76
```

To stop a peloton job by job identifier, owning team or labels
```
$./peloton job stop [<flags>] [<job>]
$./peloton job stop -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76
```

To get get pod events in reverse chronological order.
```
$./peloton pod events [<flags>] <job> <instance>
$./peloton pod events -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76 0
```

To get task logs
```
$./peloton task logs [<flags>] <job> <instance> [<taskId>]
$./peloton task logs -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76 0
```

To stop task in the job. If no instances specified, then stop all tasks
```
$./peloton task stop [<flags>] <job>
$./peloton task stop -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76 0
```

To get all the tasks of a peleton job
```
$./peloton task list [<flags>] <job>
$./peloton task list -z zookeeperURL 358fad26-73fa-43c8-a350-1e9067571a76
```

To view hosts by states:  hosts in maintenance
```
$./peloton host query [<flags>]
$./peloton -z zookeeperURL host query --states=HOST_STATE_DOWN,HOST_STATE_DRAINING
```

To update by replacing job config
```
Extra flags for update:
      --maxInstanceRetries=0     maximum instance retries to bring up the instance after updating before marking it failed.If the value is 0, the instance can be retried
                                 for infinite times.
      --maxTolerableInstanceFailures=0
                                 maximum number of instance failures tolerable before failing the update.If the value is 0, there is no limit for max failure instances
                                 andthe update is marked successful even if all of the instances fail.
      --rollbackOnFailure        rollback an update if it fails
      --start-paused             start the update in a paused state
      --opaque-data=""           opaque data provided by the user
      --in-place                 start the update with best effort in-place update

Args:
  <job>            job identifier
  <spec>           YAML job spec
  <batch-size>     batch size for the update
  <respool>        complete path of the resource pool starting from the root
  <entityVersion>  entity version for concurrency control

$./peloton job stateless replace [<flags>] <job> <spec> <batch-size> <respool> <entityVersion>
$/bin/peloton job stateless replace  -z zookeeperURL
91b1b8e5-2ba8-11e7-bc23-0242ac11000d
~/testSpec.yaml 0 /DefaultResPool 1-1-1 --in-place
```

## Job Specification

To run an application on Peloton, you need to create a job and
configure it. You provide details about the container image, the
command to run as well as resource requirements of your
application. You can also provide metadata about job, such as labels,
that can help with querying or placement. A job is composed of many
_task_ instances - Peloton will create a container for each task. Each
task can be configured independently, or may use the default
configuration of the job. Some important elements of job and task
configuration are described below, see the [API
Reference](api-reference.md) for an exhaustive list of attributes.

* **_name_** User-friendly name of the job
* **_description_** Descriptive information about the job
* **_type_** Type of the job (Batch, Service, Stateful etc.)
* **_instanceCount_** Number of task instances of the job
* **_defaultConfig_** Details about the container image, command and resource requirements
  * **_resource_** Maximum amount of resources needed by a task instance to run. Supported resources types include number of CPUs, memory size, disk size, number of file descriptors and number of GPUs.
  * **_container_** Primarily specifies the container image to run and its network configuration. Other Mesos-specific container details can also be specified here, see [Mesos API reference](http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.ContainerInfo.html).
  * **_command_** Command to run as the entry-point of the launched container. See [Mesos API Reference]() for the complete list of available attributes.
    * **_shell_** Whether the command should be run through a shell
    * **_value_** and **_arguments_** Command name and its arguments
    * **_environment_** Environment variables to set within the container
    * **_uris_** URIs of files that will be downloaded and made available to the container before launch
  * **_ports_** Static and dynamic ports to expose from the task instance. Port numbers assigned for dynamic ports are available within the container as environment variables.
  * **_constraint_** Criteria that can be used to restrict the hosts where task instances may be run
* **_instanceConfig_**	Configuration overrides for individual task instances. This is merged with _defaultConfig_ to produce the final configuration for a task.
* **_respoolID_** Resource-pool where the job (and its instances) should get resources from
* **_sla_**	Scheduling considerations for the job such as priority, preemtability etc.
* **_labels_** Key-value pairs that can be used for associating custom metadata with the job.

### Examples

- Job which runs 1 instance of _Zookeeper_ using dynamic ports
```
name: zk_server
instanceCount: 1
defaultConfig:
  resource:
    cpuLimit: 2.0
    memLimitMb: 2048
    diskLimitMb: 1024
  container:
    type: 2
    mesos:
      image: "library/zookeeper:3.4.10"
  ports:
  - name: zk_port
    envName: ZOO_PORT
  command:
    shell: true
    value: "/docker-entrypoint.sh zkServer.sh start-foreground"
sla:
  preemptible: false
respoolID: 8a5273c8-0a2a-4187-a623-334438951c72
```

- Job with 10 instances and instance-specific configuration
```
name: soporific
description: "A sleepy job"
instanceCount: 10
sla:
  priority: 22
  maxrunningTime: 300
defaultConfig:
  resource:
    cpuLimit: 1.0
    memLimitMb: 2.0
    diskLimitMb: 10
    fdLimit: 10
  command:
    shell: true
    value: 'echo "Job $PELOTON_JOB_ID $PELOTON_INSTANCE_ID" && sleep 30'
instanceConfig:
  0:
    name: task0
    command:
      shell: true
      value: 'echo "Hello instance 0" && sleep 100'
  1:
    name: task1
    command:
      shell: true
      value: 'echo "Hello instance 1" && sleep 15'
```
