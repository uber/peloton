package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

// ValidationError is the returned when a configuration fails to pass validation
type ValidationError struct {
	errorMap validator.ErrorMap
}

// ErrForField returns the validation error for the given field
func (e ValidationError) ErrForField(name string) error {
	return e.errorMap[name]
}

// Error returns the error string from a ValidationError
func (e ValidationError) Error() string {
	var w bytes.Buffer

	fmt.Fprintf(&w, "validation failed")
	for f, err := range e.errorMap {
		fmt.Fprintf(&w, "   %s: %v\n", f, err)
	}

	return w.String()
}

// Parse loads the given configFiles in order, merges them together, and parse into given
// config interface.
func Parse(config interface{}, configFiles ...string) error {
	if len(configFiles) == 0 {
		return errors.New("no files to load")
	}
	for _, fname := range configFiles {
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			return err
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return err
		}
	}

	// Validate on the merged config at the end.
	if err := validator.Validate(config); err != nil {
		return ValidationError{
			errorMap: err.(validator.ErrorMap),
		}
	}
	return nil
}
