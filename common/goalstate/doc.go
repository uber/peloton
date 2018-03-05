/*
Package goalstate implements a engine to drive a goal state machine.
Any item enqueued into the goal state engine needs to implement the
Entity interface. The enqueued entity should be able to fetch a unique
string identifier, its state and goal state, and should be able to fetch
an action list for a given state and goal state. Each action should
implement the action function interface.
The engine provides an Enqueue function which can be used to enqueue
an entity with a deadline of when it is should be dequeued for evaluation.
When its deadline expires, the action list corresponding to its state and
goal state are executed in order. If any of the actions return an error on
execution, the entity is requeued for evaluation with an exponential backoff.
*/
package goalstate
