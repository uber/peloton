package models

// Assignment represents the assignment of a task to an offer. Note that the same offer can be used in multiple
// different assignment instances.
type Assignment struct {
	offer *Offer
	task  *Task
}

// NewAssignment will create a new empty assignment from a task.
func NewAssignment(task *Task) *Assignment {
	return &Assignment{
		task: task,
	}
}

// SetOffer will set the offer in the assignment to the given offer.
func (assignment *Assignment) SetOffer(offer *Offer) {
	assignment.offer = offer
}

// Offer wil return the offer that the task was assigned to.
func (assignment *Assignment) Offer() *Offer {
	return assignment.offer
}

// Task will return the task of the assignment.
func (assignment *Assignment) Task() *Task {
	return assignment.task
}
