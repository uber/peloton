# Introduction

Cluster management, a common software infrastructure among technology companies, aggregates compute resources from a collection of physical hosts into a shared resource pool, amplifying compute power and allowing for flexible use of data center hardware. Cluster management provides an abstraction layer for various workloads. With the increasing scale of business, efficient use of cluster resources becomes very important. 

However, compute stack was underutilized due to several dedicated clusters for different use cases, such as batch (Hadoop), stateless (microservices), and stateful (including Cassandra and MySQL). In addition, the very dynamic nature of business means vast fluctuations in resources demands. These edge cases forces to over-provision hardware for each cluster in order to handle peak workloads. Relying on dedicated clusters also means that we can't share compute resources between them. During peak periods, one cluster may be starving for resources while others have resources to spare.

To make better use of our resources, we needed to co-locate these workloads on to one single compute platform. The resulting efficiencies would reduce infrastructure cost, ultimately helping the business. 

The solution we arrived at was Peloton, a unified scheduler designed to manage resources across distinct workloads, combining our separate clusters into a unified one. Peloton supports all workloads with a single shared platform, balancing resource use, elastically sharing resources, and helping plan for future capacity needs.

## Compute cluster workloads
There are generally four main categories of compute cluster workloads : stateless, stateful, batch, and daemon jobs.

- Stateless Jobs are long-running services without persistent states.
- Stateful Jobs are long-running services, such as those from Cassandra, MySQL, and Redis, that have persistent state on local disks.
- Batch Jobs typically take a few minutes to a few days to run to completion. There is a broad category of batch jobs for data analytics, machine learning, maps, and autonomous vehicles-related processing, emanating from software such as Hadoop, Spark, and TensorFlow. These jobs are preemptible by nature and less sensitive to short-term performance fluctuations due to cluster resource shortages.
- Daemon Jobs are agents running on each host for infrastructure components, such as Apache Kafka, HAProxy, and M3 Collector.

