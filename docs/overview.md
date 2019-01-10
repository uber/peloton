Overview
========

As Uber compute clusters scale, it is important to make efficient use of
cluster resources by colocating long-running and batch workloads on same
clusters. However, existing two-level scheduler architecture in Mesos
imposes several limitations in terms of task placement, resource
allocation and job preemption. Peloton is unified resource scheduler on
top of Mesos to manage compute resources more efficiently and provide
hierarchical max-min fairness guarantees for different teams. Peloton
can scale upto millions of tasks by using an active-active and scale-out
architecture.

Introduction
============

We have identified four categories of compute workloads running in Uber
datacenters, i.e. stateless services, stateful services, batch jobs and
daemon jobs. Both stateless and stateful services are long running
services that should never go down. They often handle latency-sensitive
requests that are user-facing. Stateless services are the long running
services without persistent states. For example, most of services
managed by uDeploy today are stateless, such as marketplace services,
trip services, payment services etc. Stateless services have strict SLA
and only a few instances are preemptible at a given time to avoid
user-visible service disruption. Stateful services are the long running
services that have persistent states on local disks such as Schemaless,
Cassandra, Elasticsearch, MySQL etc. They are not preemptible and also
very expensive to relocate to other hosts due to their local persistent
states.

Batch jobs typically take a few seconds to a few days to complete. There
are a broad category of batch jobs for developer experience, data
analytics, machine learning and self-driving cars such as Mobile CI,
uBuild, Spark, Hive, Piper, Tensorflow etc. They are preemptive by
nature and much less sensitive to short-term performance fluctuations
due to cluster resource shortage. Batch jobs typically require access to
distributed file systems such as HDFS, Lustre, and FreeNAS for input and
output data.

To support the use case of different workloads, we propose to develop a
Unified Resource Scheduler (code name Peloton) as a joint effort by
Compute, Data Infra and Storage teams. Peloton is built on top of Mesos
so that we can leverage Mesos to aggregate resources from different
hosts and launch tasks as docker containers. To manage cluster wide
resources more efficiently and make global scheduling decisions faster,
Peloton uses hierarchical resource pools to manage elastic resources
among different organizations and teams.

Architecture
============

![image](_static/Peloton-Architecture.png)

Picture shows the high-level architecture of Peloton built on top of
Mesos, Zookeeper and Cassandra. The Peloton system has the following
components:

Peloton UI: a Web-based UI for managing jobs, tasks, volumes and
resource pools in Peloton.

Peloton CLI: a tool to provide similar functionalities as Peloton UI but
using a command-line interface.

Peloton API: is an API layer modelled using Protocol Buffer as interface
definition language and using YARPC as RPC runtime. Both Peloton UI and
CLI will be built on top of the same Peloton API as well as other
Peloton extensions.

Host Manager: abstracts away Mesos details from other Peloton
components. It registers with Mesos via Mesos HTTP API, and implements a
generic Mesos API driver using custom YARPC transport and encoding
plugins. Host manager also extracts host related information from offers
and cache a list of hosts in Peloton. We might also use Mesos Agent API
to retrieve more host-level information such as host metrics etc.

Resource Manager: maintains the resource pool hierarchy and periodically
calculates the resource entitlement of each resource pool which is then
used to schedule or preempt tasks correspondingly. Placement Engine
finds the placement (i.e. task to host mapping) by taking into
consideration the job and task constraints as well as host attributes.
Placement engines could be pluggable for different job types such as
stateful services and batch jobs.

Job Manager: handles the lifecycle of jobs, tasks and volumes. It also
supports the rolling upgrade of tasks in a job for long-running
services.

Storage Gateway: provides an abstraction layer on top of different
storage backends so that we can migrate from one storage backend to
another without significant change in Peloton itself. We are using
ST-API in golang which has default backend for Cassandra built-in.

Group Membership: manages the set of Peloton master instances and elects
a leader to register to Mesos as a framework as well as instantiate a
resource manager.
