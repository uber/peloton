package tasks

/*
Package tasks contains the task service interface and an implementation of it. The task service is responsible for
dequeueing gangs/tasks for placement, setting placements of tasks on offers in the resource service and for enqueueing
gangs/tasks, which failed to get placed, back into the resource service. The task service is also responsible for any
kind of logging and metrics emission so that these things will not pollute the code in the placement engine main loop.
*/
