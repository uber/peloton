Peloton CLI
===============
###Overview of Peloton CLI
Peloton CLI is a command line interface for interacting with Peloton clusters.
This section covers Peloton CLI  usage and provides commands examples.
All Peloton CLI commands start with peloton and is followed by the flag, commands, and arguments.

To communicate with a Peloton cluster, you need to provide the zookeeper
information or the cluster name you want to access.

###Requirement
You need to have a working Peloton binary installed and a Peloton cluster.

###Usage
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

