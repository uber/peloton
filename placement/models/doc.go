package models

/*
Package models contains all the data containers used to wrap the Peloton HostOffer, Task and Placement types in order to
be able to map them to the corresponding Mimir types and to host them to each individual placement strategy.

This package contains the following types:
  * Assignment - is in 1:1 correspondence with a Task and it also keeps a reference to the Host on which the task is
                 is currently assigned if any. Note that there can be many assignments that references the same host,
                 this models that each host can be assigned multiple tasks in the same placement round.
  * Host - is in 1:1 correspondence with a Peloton HostOffer, in addition it also holds the tasks that is already
            running on the host that the host belongs to and the time when the host was claimed from the host manager.
  * Task - is in 1:1 correspondence with a Peloton Task, in addition it also holds a reference to the gang that the task
           belongs to. The task also keeps a deadline for when the task should not spent any more time being placed.
           The task also knows the number of placement rounds where it was successfully assigned an host and how many
           it maximally wants to participate in.
  * PortRange - is used in the placement engine main loop to allocate ports to any task that needs them. In the
                placement rounds only the number of used and remaining ports are tracked, but the actual assignment is
                taking place in the placement engine main loop and not in the individual placement strategy.
*/
