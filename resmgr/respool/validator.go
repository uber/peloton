package respool

// Validator performs validations on the resource config pool
type Validator interface {
	// Validate validates the resource pool config
	Validate(data interface{}) error
	// Register a slice of validator functions
	Register(validatorFuncs interface{}) (Validator, error)
}
