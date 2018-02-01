.. _userguide:

User Guide
==========

Api Doc
--------

- See the complete `API documentation <_static/docs.html>`_

Resource Pools
----------------
Resource Pool is a logical abstraction for a subset of resources in a cluster. All resources in a cluster can be divided into hierarchical resource pools based on organizations and teams. A resource pool can contain child resource pools to further divide the resources within an organization. Resource sharing among pools are elastic in nature. Resource pools with high demand can borrow resources from other pools if they are not using those resources. When a user submits a job, she must provide a leaf resource pool where the resources of the job will be accounted for. The resource pool tree is typically managed by Compute SRE team but the management of the sub-trees within an organization can be delegated to the organization’s SRE team. 

Every resource pool has different resource dimensions, such as CPU, Memory, Disk Size and GPU. The number of resource dimensions will increase in the future when Mesos supports more resource isolations such as Disk IO.There are three basic resource controls for each resource dimension in a resource pool, i.e. Reservation, Limit, and Share. These settings can be used to provide different resource allocation policies for organizations.

Reservation is the minimal guarantee of resources for a resource pool. Cumulative resource reservation across all pools can not be more than a cluster capacity. This has to be configured carefully as that would be the guiding factor for defining and meeting SLA’s. 

Limit is the maximum resources a resource pool can consume at any given time. Every resource pool can expand more than its reservation to this limit in case the cluster is free. At some point if more guaranteed workloads in other pools come to the cluster then we will give back those resources. These resources (which are more than the reservation) will not be guaranteed and is revocable in nature.

Share specifies the relative weight a resource pool is entitled to allocate when there is free capacity in the cluster.  Similarly when preemption will kick in it will preempt the workloads based on the share as well. Least share will have more preemption.

Entitlement is the amount of resources which each resource pool can use across different resource hierarchies at any given point of time. This is a changing value which is based on demand in the resource pool and free resources in the cluster at that moment.

Allocation is the amount of resources which each resource pool is using (Allocated) at any given point of time.
 
.. image:: _static/Resource-Pool.png

Picture above shows a ATG resource pool with two child resource pools, i.e. AVMaps and Simulation. All resources in the cluster will be divided between these two organizations. Simulation is a leaf resource pool since it does not have any teams. So users are allowed to submit jobs at Simulation resource pool. However AvMaps has two child resource pools for AV-Build and AV-Release, so the resources in AVMaps will be further divided based on the resource pool settings of AV-Build and AV-Release.

Preemption
-------------
Task preemptor makes sure that each resource pool is not using more resources than what it is entitled for. If a resource pool has less demand than its reservation, its reserved resources will be given to some other resource pools. However, if the resource pool starts having more demand in next scheduling cycle than task preemptor will have to preempt tasks from other pools which are using more resources than their current entitlement.

There are two types of preemptions which needs to occur into the system:

Inter resource pool preemption: This will enforce the max min fairness across all resource pools.This will apply preemption policies on the resources and claim back resources from the resource pools. The Admin should be able to plug in different preemption policies and preemption will happen based on the policy. We will try to use the Min-Max Fairness preemption Policy for the inter resource pool preemption.

Intra resource pool preemption:  This will enforce entitlement within the resource pool. Every pool could have many users and each of them has many jobs running. It may often happen that one user can use the capacity of the full resource pool and other users will wait for those jobs to finish. This will lead to SLA miss for the jobs which are stuck in the pool. Other scenario is, it may happen that the lower priority jobs are running and if higher priority job comes in then the scheduler has to make space for the higher priority jobs.


Peloton Resources
-----------------

- `Peloton RFC <https://docs.google.com/document/d/174TjLbnJ7z9HdgMKvbeddCq4D8iScR5kVG4u2HANl6c/edit?usp=sharing>`_
- `Preemption <https://docs.google.com/document/d/1M-tTDJn6YLH4pDmZUv5tPz3sBUU5cWJPX-BvPHrWcXg/edit?usp=sharing>`_

