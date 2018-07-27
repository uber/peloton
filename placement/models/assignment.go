package models

// Assignment represents the assignment of a task to a host.
// One host can be used in multiple assignments.
type Assignment struct {
	HostOffers *HostOffers `json:"host"`
	Task       *Task       `json:"task"`
}

// GetHost returns the host that the task was assigned to.
func (assignment *Assignment) GetHost() *HostOffers {
	return assignment.HostOffers
}

// SetHost sets the host in the assignment to the given host.
func (assignment *Assignment) SetHost(host *HostOffers) {
	assignment.HostOffers = host
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
