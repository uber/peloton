/*
Package respool is responsible for
1. Building and maintaining the in-memory resource pool tree and perform
   operations it.
2. Providing API handlers to operate on the resource pool hierarchy.
3. Performing admission control of gangs in the resource pool.

Each leaf resource pool maintains 2 queues
1. Pending queue: containing all the incoming/enqueued tasks into the
resource pool.
2. Controller queue: containing controller tasks which
are transferred from the pending queue if they can't be admitted.


Admission cycle

During each scheduling cycle the scheduler loops through all the leaf
resource pool nodes and dequeques gangs from the resource pool.
Each resource pool is responsible for the doing
the admission control with its 2 queues. Here's how it works:

1. The resource pool first dequeues gangs from the controller queue and tries to admit as many
   controller tasks it can. This is decided by the max_controller_percentage, which
   is defined as the maximum resources the controller tasks can use a percentage of the resource
   pools reservation. For eg if the max_controller_percentage=10 and resource pool's reservation is:
   cpu:100
   mem:1000
   disk:1000
   gpu:10

   Then the maximum resources the controller tasks can use is 10% of the reservation, i.e.:
   cpu:10
   mem:100
   disk:100
   gpu:1

   If these resources are not used by the controller tasks they will be given to other tasks.
   Once the controller task can be admitted it is removed from the controller queue and the resoures
   are added to the controller allocation. One we have exhausted all the controller tasks which
   can be admitted we move on to the second phase.

2. We now look at the pending queue to admit the rest of the tasks. The resource pool keeps deuqueueing gangs
   until it reaches a gang which can't be admitted. At this point we check if the gang is of controller task
   and if so, the admission controller moves this gang from the pending queue to the controller queue and moves
   on to the next task. Once it reaches a task which can't be admitted and is *not* a controller task then it
   stops the admission control and returns the list of gangs to the scheduler.
*/
package respool
