# Peloton API Guide

This section describes different clients supported for Peloton API.
Currently, we support three clients: Python, Golang and Java.

There are four types of clients internal to Peloton client: Job, Task, ResPool,
Update and Host. We will focus on the Job client API in this document.

To communicate with a Peloton cluster, you need to provide the zookeeper
information or the cluster name you want to access to the client. The doc
assumes that the Peloton cluster is setup and the zookeeper information is
available to the user.

## Python

Python client is available on PyPi server. It can be installed using pip.
```
  $ cat requirements.txt
  peloton-client~=0.8.0
  $ pip install -r requirements.txt
```

### Example Python client usage:

Sections below provide some sample code to make API calls using Python client.
Imports not included.


* Create the client
```
from peloton_client.client import PelotonClient

client = PelotonClient(
    name='peloton-client',
    zk_servers=<List of ZooKeeper servers>,
)
```

* Create Job
```
request = job.CreateRequest(
    config=job.JobConfig(
      name='TestPelotonJob',
      owningTeam='engineering',
      description='test job from python client',
      instanceCount=3,
      defaultConfig=task.TaskConfig(
        resource=task.ResourceConfig(
          cpuLimit=0.1,
          memLimitMb=128.0,
        ),
        container=mesos.ContainerInfo(
          type='DOCKER',
        ),
        command=mesos.CommandInfo(
          shell=True,
          value="echo 'hello' &  sleep 30",
        ),
      ),
      respoolID=peloton.ResourcePoolID(value=<resource pool id>),
    ),
  )
resp = client.job_svc.Create(request)
```

* Get Job
```
request = job.GetRequest(
  id=peloton.JobID(value=job_id),
)
resp = client.job_svc.Get(request)
```

* Query Jobs
```
request = job.QueryRequest(
  spec=job.QuerySpec(
    jobStates=[job.INITIALIZED, job.PENDING, job.RUNNING],
    pagination=query.PaginationSpec(offset=0,limit=100),
  ),
)
resp = client.job_svc.Query(request)
```

## Golang

Go client can be installed using glide.

* Create the client (imports, error handling not included to keep this brief)
```
  discovery, err := leader.NewZkServiceDiscovery(
    <list of zookeeper servers>, <zookeeper root>)

  client, err = NewClient("peloton-test-client", discovery)
```

### Example Golang client usage:

Sections below provide some sample code to make API calls using Golang client.
Imports not included.

* Create Job
```
  var jobConfig job.JobConfig
  buffer, err := ioutil.ReadFile(<config yaml file>)
  err = yaml.Unmarshal(buffer, &jobConfig)

  // set the resource pool ID
  jobConfig.RespoolID = <resource pool ID>

  request := &job.CreateRequest{
    Id: &peloton.JobID{Value: <JOB UUID of choice>},
    Config: &jobConfig,
  }

  response, err := client.JobManager().Create(context.Background(), request)
```

* Get Job
```
  request = &job.GetRequest{
    Id: &peloton.JobID{Value: <Job UUID>},
  }
  resp, err := client.JobManager().Get(context.Background(), request)
```

## Java

* Create the client
```
  JobManagerGrpc.JobManagerBlockingStub jobManager = new JobManager(
    <client name string>, <List of ZooKeeper servers>).blockingConnect();
```

### Example Java client usage:

Sample API calls using Java client

* Create Job
```
Protos.CommandInfo commandInfo = Protos.CommandInfo.newBuilder()
  .setValue(<command line string)
  .build();
Protos.ContainerInfo container = Protos.ContainerInfo.newBuilder()
  .setType(Protos.ContainerInfo.Type.DOCKER)
  .setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
  .setImage(<docker image url>))
  .build();
Task.ResourceConfig resource = Task.ResourceConfig.newBuilder()
  .setCpuLimit(1.0)
  .setMemLimitMb(512)
  .build();
Task.TaskConfig defaultTaskConfig = Task.TaskConfig.newBuilder()
  .setContainer(container)
  .setCommand(commandInfo)
  .setResource(resource)
  .build();
Peloton.ResourcePoolID poolId = <Resource pool ID>

Job.JobConfig config = Job.JobConfig.newBuilder()
  .setName("ExampleJob")
  .setInstanceCount(1)
  .setRespoolID(poolId)
  .setDescription("Small test job from Java.")
  .setDefaultConfig(defaultTaskConfig)
  .setType(Job.JobType.BATCH)
  .build();

Job.CreateRequest request = Job.CreateRequest.newBuilder()
  .setConfig(config)
  .setId(<Job UUID>)
  .build();

Job.CreateResponse response = jobManager.create(request);
```

* Get Job
```
Job.GetRequest request = Job.GetRequest.newBuilder()
  .setId(<Job UUID>)
  .build();

Job.GetResponse response = jobManager.get(request);
```
