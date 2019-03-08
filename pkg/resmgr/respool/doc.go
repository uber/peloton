// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
3. Non-Preemptible queue: non-preemptible tasks are moved to this queue if
not admissible.
4. Revocable queue: revocable tasks if can not be admitted are moved to this queue,
primary goal to maintain revocable queue is to unblock non-revocable tasks
from admitting.


Admission cycle

Each resource pool, leaf or non-leaf, maintains an in-memory resource entitlement
for non-revocable and revocable tasks. Entitlement is calculated outside
this package, resource pool exposes API to calculate them.
Entitlement is calculated, based on resource pools limit, reservation,
slack limit, demand (resource requried)  for pending tasks and
allocation (already admitted tasks).

Admission cycle iterates over above mentioned 4 queues in following sequeunce
for each leaf resource pools
1) Non-Preemptible
2) Controller
3) Revocable
4) Pending

For gangs dequeued from each queue, admission control validates 3 conditions
before admitting the task
1) Sufficient entitlement based on task type (revocable or non-revocable).
2) Non-Prememtible tasks do not use resources more than Resource pool reservation.
3) Controller tasks do not use resources more than controller limit.

Below is an example for admission control, on how it works:

1. Resource pools first dequeus gangs from non-preemptible queue, and tries to admit as many
non-preemptible (which are also non-revocalbe tasks) untill resource pool reservation.

Resource pool reservation:
   cpu:100
   mem:1000
   disk:1000
   gpu:10

Task requirement:
   cpu:10
   mem:100
   disk:100
   gpu:1

10 non-preemptible tasks are admittable, if none is already running. If some resources are
already allocated to non-preemptible tasks then fewer will be admitted.

If resources are allocated to revocable tasks, then they will be preempted for non-preemptible tasks.

2. The resource pool then dequeues gangs from the controller queue and tries to admit as many
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

3. Resource pool then dequeus from revocable queue, revocable tasks are admitted using slack entitlement.
Resources available for revocable tasks are constraint to slack limit. Behavior is some what comparable to
controller limit.
Resource pool reservation
   cpu:100
   mem:1000
   disk:1000
   gpu:10

Slack limit = 30%, i.e. max resource available for revocable tasks are 30% of reservation
   cpu:30
   mem:300
   disk:300
   gpu:3

Slack limit is enforced to prevent revocable tasks from hogging all resources in resource pool.
Secondly, slack limit does not guarantee resources to revocable tasks, as they are best effort
in behavior and have lower preference then non-revocable tasks.

4. We now look at the pending queue to admit the rest of the tasks. The resource pool keeps deuqueueing gangs
   until it reaches a gang which can't be admitted.
   - At this point, if a task is non-preemptible, then it is moved to non-preemptible queue.
   - If a task is controller task then it is moved to controller queue.
   - Similarly, if a task is revocable task then moved to revocable queue, to unblock
   non-revocable tasks.
   - Once it reaches a task which can't be admitted and is *not* a controller task,
   non-preemptible or revocable tasks then it stops the admission control
   and returns the list of gangs to the scheduler.
*/
package respool
