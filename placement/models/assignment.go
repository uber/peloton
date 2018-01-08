package models

// Assignment represents the assignment of a task to an host. Note that the same host can be used in multiple
// different assignment instances.
type Assignment struct {
	Host *Host `json:"host"`
	Task *Task `json:"task"`
}

// GetHost returns the host that the task was assigned to.
func (assignment *Assignment) GetHost() *Host {
	return assignment.Host
}

// SetHost sets the host in the assignment to the given host.
func (assignment *Assignment) SetHost(host *Host) {
	assignment.Host = host
}

// GetTask returns the task of the assignment.
func (assignment *Assignment) GetTask() *Task {
	return assignment.Task
}

// SetTask sets the task in the assignment to the given task.
func (assignment *Assignment) SetTask(task *Task) {
	assignment.Task = task
}

// NewAssignment will create a new empty assignment from a task.
func NewAssignment(task *Task) *Assignment {
	return &Assignment{
		Task: task,
	}
}
