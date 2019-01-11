### Type of Jobs
Peloton supports for the following four types of jobs - batch, stateless, stateful and daemon.

**Batch Jobs** are jobs which typically take a few seconds to a few days to complete. There are a broad category of batch jobs for developer
experience, data analytics, machine learning etc such as Spark, Hive, Tensorflow etc. These jobs tend to be less sensitive to short-term performance fluctuations due to cluster resource shortage, and quite often are preemptible by nature.

**Stateless Jobs** are long running services which should never go down and have no persistent state on local disk. Most web backend applications belong to this category. These jobs have a strict SLA and only few of the instances of this job are allowed to be unavailable at a given time.

**Stateful Jobs** are also long running services which should never go down, but these jobs have persistent states on local disks. Examples of such jobs include Cassandra, Redis, MySQL etc. These jobs are not only not preemptible but are also very expensive to relocate to other hosts due to their local persistent states.

**Daemon Jobs** are the agents running on each host for infrastructure components such as for statistics collection. Daemon jobs are neither preemptible nor relocatable.
