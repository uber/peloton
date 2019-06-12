// Code generated by thriftrw v1.19.1. DO NOT EDIT.
// @generated

package api

import (
	errors "errors"
	fmt "fmt"
	multierr "go.uber.org/multierr"
	wire "go.uber.org/thriftrw/wire"
	zapcore "go.uber.org/zap/zapcore"
	strings "strings"
)

// ReadOnlyScheduler_GetJobUpdateSummaries_Args represents the arguments for the ReadOnlyScheduler.getJobUpdateSummaries function.
//
// The arguments for getJobUpdateSummaries are sent and received over the wire as this struct.
type ReadOnlyScheduler_GetJobUpdateSummaries_Args struct {
	JobUpdateQuery *JobUpdateQuery `json:"jobUpdateQuery,omitempty"`
}

// ToWire translates a ReadOnlyScheduler_GetJobUpdateSummaries_Args struct into a Thrift-level intermediate
// representation. This intermediate representation may be serialized
// into bytes using a ThriftRW protocol implementation.
//
// An error is returned if the struct or any of its fields failed to
// validate.
//
//   x, err := v.ToWire()
//   if err != nil {
//     return err
//   }
//
//   if err := binaryProtocol.Encode(x, writer); err != nil {
//     return err
//   }
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) ToWire() (wire.Value, error) {
	var (
		fields [1]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.JobUpdateQuery != nil {
		w, err = v.JobUpdateQuery.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 1, Value: w}
		i++
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a ReadOnlyScheduler_GetJobUpdateSummaries_Args struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a ReadOnlyScheduler_GetJobUpdateSummaries_Args struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v ReadOnlyScheduler_GetJobUpdateSummaries_Args
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 1:
			if field.Value.Type() == wire.TStruct {
				v.JobUpdateQuery, err = _JobUpdateQuery_Read(field.Value)
				if err != nil {
					return err
				}

			}
		}
	}

	return nil
}

// String returns a readable string representation of a ReadOnlyScheduler_GetJobUpdateSummaries_Args
// struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.JobUpdateQuery != nil {
		fields[i] = fmt.Sprintf("JobUpdateQuery: %v", v.JobUpdateQuery)
		i++
	}

	return fmt.Sprintf("ReadOnlyScheduler_GetJobUpdateSummaries_Args{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this ReadOnlyScheduler_GetJobUpdateSummaries_Args match the
// provided ReadOnlyScheduler_GetJobUpdateSummaries_Args.
//
// This function performs a deep comparison.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) Equals(rhs *ReadOnlyScheduler_GetJobUpdateSummaries_Args) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !((v.JobUpdateQuery == nil && rhs.JobUpdateQuery == nil) || (v.JobUpdateQuery != nil && rhs.JobUpdateQuery != nil && v.JobUpdateQuery.Equals(rhs.JobUpdateQuery))) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of ReadOnlyScheduler_GetJobUpdateSummaries_Args.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.JobUpdateQuery != nil {
		err = multierr.Append(err, enc.AddObject("jobUpdateQuery", v.JobUpdateQuery))
	}
	return err
}

// GetJobUpdateQuery returns the value of JobUpdateQuery if it is set or its
// zero value if it is unset.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) GetJobUpdateQuery() (o *JobUpdateQuery) {
	if v != nil && v.JobUpdateQuery != nil {
		return v.JobUpdateQuery
	}

	return
}

// IsSetJobUpdateQuery returns true if JobUpdateQuery is not nil.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) IsSetJobUpdateQuery() bool {
	return v != nil && v.JobUpdateQuery != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the arguments.
//
// This will always be "getJobUpdateSummaries" for this struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) MethodName() string {
	return "getJobUpdateSummaries"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Call for this struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

// ReadOnlyScheduler_GetJobUpdateSummaries_Helper provides functions that aid in handling the
// parameters and return values of the ReadOnlyScheduler.getJobUpdateSummaries
// function.
var ReadOnlyScheduler_GetJobUpdateSummaries_Helper = struct {
	// Args accepts the parameters of getJobUpdateSummaries in-order and returns
	// the arguments struct for the function.
	Args func(
		jobUpdateQuery *JobUpdateQuery,
	) *ReadOnlyScheduler_GetJobUpdateSummaries_Args

	// IsException returns true if the given error can be thrown
	// by getJobUpdateSummaries.
	//
	// An error can be thrown by getJobUpdateSummaries only if the
	// corresponding exception type was mentioned in the 'throws'
	// section for it in the Thrift file.
	IsException func(error) bool

	// WrapResponse returns the result struct for getJobUpdateSummaries
	// given its return value and error.
	//
	// This allows mapping values and errors returned by
	// getJobUpdateSummaries into a serializable result struct.
	// WrapResponse returns a non-nil error if the provided
	// error cannot be thrown by getJobUpdateSummaries
	//
	//   value, err := getJobUpdateSummaries(args)
	//   result, err := ReadOnlyScheduler_GetJobUpdateSummaries_Helper.WrapResponse(value, err)
	//   if err != nil {
	//     return fmt.Errorf("unexpected error from getJobUpdateSummaries: %v", err)
	//   }
	//   serialize(result)
	WrapResponse func(*Response, error) (*ReadOnlyScheduler_GetJobUpdateSummaries_Result, error)

	// UnwrapResponse takes the result struct for getJobUpdateSummaries
	// and returns the value or error returned by it.
	//
	// The error is non-nil only if getJobUpdateSummaries threw an
	// exception.
	//
	//   result := deserialize(bytes)
	//   value, err := ReadOnlyScheduler_GetJobUpdateSummaries_Helper.UnwrapResponse(result)
	UnwrapResponse func(*ReadOnlyScheduler_GetJobUpdateSummaries_Result) (*Response, error)
}{}

func init() {
	ReadOnlyScheduler_GetJobUpdateSummaries_Helper.Args = func(
		jobUpdateQuery *JobUpdateQuery,
	) *ReadOnlyScheduler_GetJobUpdateSummaries_Args {
		return &ReadOnlyScheduler_GetJobUpdateSummaries_Args{
			JobUpdateQuery: jobUpdateQuery,
		}
	}

	ReadOnlyScheduler_GetJobUpdateSummaries_Helper.IsException = func(err error) bool {
		switch err.(type) {
		default:
			return false
		}
	}

	ReadOnlyScheduler_GetJobUpdateSummaries_Helper.WrapResponse = func(success *Response, err error) (*ReadOnlyScheduler_GetJobUpdateSummaries_Result, error) {
		if err == nil {
			return &ReadOnlyScheduler_GetJobUpdateSummaries_Result{Success: success}, nil
		}

		return nil, err
	}
	ReadOnlyScheduler_GetJobUpdateSummaries_Helper.UnwrapResponse = func(result *ReadOnlyScheduler_GetJobUpdateSummaries_Result) (success *Response, err error) {

		if result.Success != nil {
			success = result.Success
			return
		}

		err = errors.New("expected a non-void result")
		return
	}

}

// ReadOnlyScheduler_GetJobUpdateSummaries_Result represents the result of a ReadOnlyScheduler.getJobUpdateSummaries function call.
//
// The result of a getJobUpdateSummaries execution is sent and received over the wire as this struct.
//
// Success is set only if the function did not throw an exception.
type ReadOnlyScheduler_GetJobUpdateSummaries_Result struct {
	// Value returned by getJobUpdateSummaries after a successful execution.
	Success *Response `json:"success,omitempty"`
}

// ToWire translates a ReadOnlyScheduler_GetJobUpdateSummaries_Result struct into a Thrift-level intermediate
// representation. This intermediate representation may be serialized
// into bytes using a ThriftRW protocol implementation.
//
// An error is returned if the struct or any of its fields failed to
// validate.
//
//   x, err := v.ToWire()
//   if err != nil {
//     return err
//   }
//
//   if err := binaryProtocol.Encode(x, writer); err != nil {
//     return err
//   }
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) ToWire() (wire.Value, error) {
	var (
		fields [1]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.Success != nil {
		w, err = v.Success.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 0, Value: w}
		i++
	}

	if i != 1 {
		return wire.Value{}, fmt.Errorf("ReadOnlyScheduler_GetJobUpdateSummaries_Result should have exactly one field: got %v fields", i)
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a ReadOnlyScheduler_GetJobUpdateSummaries_Result struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a ReadOnlyScheduler_GetJobUpdateSummaries_Result struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v ReadOnlyScheduler_GetJobUpdateSummaries_Result
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 0:
			if field.Value.Type() == wire.TStruct {
				v.Success, err = _Response_Read(field.Value)
				if err != nil {
					return err
				}

			}
		}
	}

	count := 0
	if v.Success != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf("ReadOnlyScheduler_GetJobUpdateSummaries_Result should have exactly one field: got %v fields", count)
	}

	return nil
}

// String returns a readable string representation of a ReadOnlyScheduler_GetJobUpdateSummaries_Result
// struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Success != nil {
		fields[i] = fmt.Sprintf("Success: %v", v.Success)
		i++
	}

	return fmt.Sprintf("ReadOnlyScheduler_GetJobUpdateSummaries_Result{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this ReadOnlyScheduler_GetJobUpdateSummaries_Result match the
// provided ReadOnlyScheduler_GetJobUpdateSummaries_Result.
//
// This function performs a deep comparison.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) Equals(rhs *ReadOnlyScheduler_GetJobUpdateSummaries_Result) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !((v.Success == nil && rhs.Success == nil) || (v.Success != nil && rhs.Success != nil && v.Success.Equals(rhs.Success))) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of ReadOnlyScheduler_GetJobUpdateSummaries_Result.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.Success != nil {
		err = multierr.Append(err, enc.AddObject("success", v.Success))
	}
	return err
}

// GetSuccess returns the value of Success if it is set or its
// zero value if it is unset.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) GetSuccess() (o *Response) {
	if v != nil && v.Success != nil {
		return v.Success
	}

	return
}

// IsSetSuccess returns true if Success is not nil.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) IsSetSuccess() bool {
	return v != nil && v.Success != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the result.
//
// This will always be "getJobUpdateSummaries" for this struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) MethodName() string {
	return "getJobUpdateSummaries"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Reply for this struct.
func (v *ReadOnlyScheduler_GetJobUpdateSummaries_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
