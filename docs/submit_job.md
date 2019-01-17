### How to Submit Jobs
This sections discusses how to submit jobs to peloton via peloton cli as well as via a peloton client.
#### Submit via Peloton CLI
To submit a batch job to Peloton, use the following CLI command.
```
peloton job create --help
usage: peloton job create [<flags>] <respool> <config>

create a job

Flags:
  -h, --help                     Show context-sensitive help (also try --help-long and --help-man).
  -j, --json                     print full json responses
  -m, --jobmgr=localhost:5392    name of the jobmgr address to use (grpc) (set $JOBMGR_URL to override)
  -v, --resmgr=localhost:5394    name of the resource manager address to use (grpc) (set $RESMGR_URL to override)
  -u, --hostmgr=localhost:5391   name of the host manager address to use (grpc) (set $HOSTMGR_URL to override)
  -e, --clusterName=CLUSTERNAME  name of the cluster you want to connect to.e.g pit1-preprod01
  -z, --zkservers=ZKSERVERS ...  zookeeper servers used for peloton service discovery. Specify multiple times for multiple servers(set $ZK_SERVERS to override with ' ' as delimiter)
      --zkroot="/peloton"        zookeeper root path for peloton service discovery(set $ZK_ROOT to override)
  -t, --timeout=20s              default RPC timeout (set $TIMEOUT to override)
      --version                  Show application version.
  -i, --jobID=JOBID              optional job identifier, must be UUID format
      --secret-path=""           secret mount path
      --secret-data=""           secret data string

Args:
  <respool>  complete path of the resource pool starting from the root
  <config>   YAML job configuration
```

An example run of this command is as follows.
```
peloton job create /DefaultResourcePool example_batch.yaml
```
where example_batch.yaml is the job configuration file and /DefaultResourcePool is the resource pool in which the job should be created.

To submit a stateless job to Peloton, use the following CLI command.
```
peloton job stateless create --help
usage: peloton job stateless create [<flags>] <respool> <spec>

create stateless job

Flags:
  -h, --help                     Show context-sensitive help (also try --help-long and --help-man).
  -j, --json                     print full json responses
  -m, --jobmgr=localhost:5392    name of the jobmgr address to use (grpc) (set $JOBMGR_URL to override)
  -v, --resmgr=localhost:5394    name of the resource manager address to use (grpc) (set $RESMGR_URL to override)
  -u, --hostmgr=localhost:5391   name of the host manager address to use (grpc) (set $HOSTMGR_URL to override)
  -e, --clusterName=CLUSTERNAME  name of the cluster you want to connect to.e.g pit1-preprod01
  -z, --zkservers=ZKSERVERS ...  zookeeper servers used for peloton service discovery. Specify multiple times for multiple servers(set $ZK_SERVERS to override with ' ' as delimiter)
      --zkroot="/peloton"        zookeeper root path for peloton service discovery(set $ZK_ROOT to override)
  -t, --timeout=20s              default RPC timeout (set $TIMEOUT to override)
      --version                  Show application version.
  -i, --jobID=JOBID              optional job identifier, must be UUID format
      --secret-path=""           secret mount path
      --secret-data=""           secret data string

Args:
  <respool>  complete path of the resource pool starting from the root
  <spec>     YAML job specification
```
  
An example run of this command is as follows.
```
peloton job create /DefaultResourcePool example_stateless.yaml
```
where example_stateless.yaml is the job configuration file and /DefaultResourcePool is the resource pool in which the job should be created.

#### Submit via Peloton Client
Users can submit job via APIs through any one of the Peloton clients. For example, for a Golang client, users can use the job.CreateRequest API to submit a batch job and the job.stateless.svc.CreateJob API to create a stateless job.
Following is sample go code to submit a batch job.
```
import "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
import "github.com/uber/peloton/.gen/peloton/api/v0/job"
...
  var jobConfig job.JobConfig
  buffer, err := ioutil.ReadFile(<config yaml file>)
  err = yaml.Unmarshal(buffer, &jobConfig)
  request := &job.CreateRequest{
    Id: &peloton.JobID{Value: <JOB UUID of choice>},
    Config: &jobConfig,
  }
  response, err := client.JobManager().Create(context.Background(), request)
```
  
Following is sample go code to submit a stateless job.
```
import "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
import "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
import "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
...
  var jobSpec stateless.JobSpec
  buffer, err := ioutil.ReadFile(<config yaml file>)
  err = yaml.Unmarshal(buffer, &jobConfig)
  request := &svc.CreateJobRequest{
    JobId: &peloton.JobID{Value: <JOB UUID of choice>},
    Spec: &jobConfig,
  }
  response, err := client.JobManager().CreateJob(context.Background(), request)
```
