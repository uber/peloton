package models

// Assignment represents the assignment of a task to an host. Note that the same host can be used in multiple
// different assignment instances.
type Assignment struct {
	host *Host
	task *Task
}

// NewAssignment will create a new empty assignment from a task.
func NewAssignment(task *Task) *Assignment {
	return &Assignment{
		task: task,
	}
}

// SetHost will set the host in the assignment to the given host.
func (assignment *Assignment) SetHost(offer *Host) {
	assignment.host = offer
}

// Host wil return the host that the task was assigned to.
func (assignment *Assignment) Host() *Host {
	return assignment.host
}

// Task will return the task of the assignment.
func (assignment *Assignment) Task() *Task {
	return assignment.task
}
