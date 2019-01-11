Job Configuration
=================
 To run an application on Peloton, you need to create a job and configure it. You provide details about the container image, the command to run as well as resource requirements of your application. You can also provide metadata about job, such as labels, that can help with querying or placement. A job is composed of many _task_ instances - Peloton will create a container for each task. Each task can be configured independently, or may use the default configuration of the job. Some important elements of job and task configuration are described below, see the [API Reference](api_ref.html) for an exhaustive list of attributes.

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
1. Job which runs 1 instance of _Zookeeper_ using dynamic ports
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
2. Job with 10 instances and instance-specific configuration
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
