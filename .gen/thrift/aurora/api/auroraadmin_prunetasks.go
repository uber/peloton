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

// AuroraAdmin_PruneTasks_Args represents the arguments for the AuroraAdmin.pruneTasks function.
//
// The arguments for pruneTasks are sent and received over the wire as this struct.
type AuroraAdmin_PruneTasks_Args struct {
	Query *TaskQuery `json:"query,omitempty"`
}

// ToWire translates a AuroraAdmin_PruneTasks_Args struct into a Thrift-level intermediate
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
func (v *AuroraAdmin_PruneTasks_Args) ToWire() (wire.Value, error) {
	var (
		fields [1]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.Query != nil {
		w, err = v.Query.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 1, Value: w}
		i++
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraAdmin_PruneTasks_Args struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraAdmin_PruneTasks_Args struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraAdmin_PruneTasks_Args
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraAdmin_PruneTasks_Args) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 1:
			if field.Value.Type() == wire.TStruct {
				v.Query, err = _TaskQuery_Read(field.Value)
				if err != nil {
					return err
				}

			}
		}
	}

	return nil
}

// String returns a readable string representation of a AuroraAdmin_PruneTasks_Args
// struct.
func (v *AuroraAdmin_PruneTasks_Args) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Query != nil {
		fields[i] = fmt.Sprintf("Query: %v", v.Query)
		i++
	}

	return fmt.Sprintf("AuroraAdmin_PruneTasks_Args{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraAdmin_PruneTasks_Args match the
// provided AuroraAdmin_PruneTasks_Args.
//
// This function performs a deep comparison.
func (v *AuroraAdmin_PruneTasks_Args) Equals(rhs *AuroraAdmin_PruneTasks_Args) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !((v.Query == nil && rhs.Query == nil) || (v.Query != nil && rhs.Query != nil && v.Query.Equals(rhs.Query))) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of AuroraAdmin_PruneTasks_Args.
func (v *AuroraAdmin_PruneTasks_Args) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.Query != nil {
		err = multierr.Append(err, enc.AddObject("query", v.Query))
	}
	return err
}

// GetQuery returns the value of Query if it is set or its
// zero value if it is unset.
func (v *AuroraAdmin_PruneTasks_Args) GetQuery() (o *TaskQuery) {
	if v != nil && v.Query != nil {
		return v.Query
	}

	return
}

// IsSetQuery returns true if Query is not nil.
func (v *AuroraAdmin_PruneTasks_Args) IsSetQuery() bool {
	return v != nil && v.Query != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the arguments.
//
// This will always be "pruneTasks" for this struct.
func (v *AuroraAdmin_PruneTasks_Args) MethodName() string {
	return "pruneTasks"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Call for this struct.
func (v *AuroraAdmin_PruneTasks_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

// AuroraAdmin_PruneTasks_Helper provides functions that aid in handling the
// parameters and return values of the AuroraAdmin.pruneTasks
// function.
var AuroraAdmin_PruneTasks_Helper = struct {
	// Args accepts the parameters of pruneTasks in-order and returns
	// the arguments struct for the function.
	Args func(
		query *TaskQuery,
	) *AuroraAdmin_PruneTasks_Args

	// IsException returns true if the given error can be thrown
	// by pruneTasks.
	//
	// An error can be thrown by pruneTasks only if the
	// corresponding exception type was mentioned in the 'throws'
	// section for it in the Thrift file.
	IsException func(error) bool

	// WrapResponse returns the result struct for pruneTasks
	// given its return value and error.
	//
	// This allows mapping values and errors returned by
	// pruneTasks into a serializable result struct.
	// WrapResponse returns a non-nil error if the provided
	// error cannot be thrown by pruneTasks
	//
	//   value, err := pruneTasks(args)
	//   result, err := AuroraAdmin_PruneTasks_Helper.WrapResponse(value, err)
	//   if err != nil {
	//     return fmt.Errorf("unexpected error from pruneTasks: %v", err)
	//   }
	//   serialize(result)
	WrapResponse func(*Response, error) (*AuroraAdmin_PruneTasks_Result, error)

	// UnwrapResponse takes the result struct for pruneTasks
	// and returns the value or error returned by it.
	//
	// The error is non-nil only if pruneTasks threw an
	// exception.
	//
	//   result := deserialize(bytes)
	//   value, err := AuroraAdmin_PruneTasks_Helper.UnwrapResponse(result)
	UnwrapResponse func(*AuroraAdmin_PruneTasks_Result) (*Response, error)
}{}

func init() {
	AuroraAdmin_PruneTasks_Helper.Args = func(
		query *TaskQuery,
	) *AuroraAdmin_PruneTasks_Args {
		return &AuroraAdmin_PruneTasks_Args{
			Query: query,
		}
	}

	AuroraAdmin_PruneTasks_Helper.IsException = func(err error) bool {
		switch err.(type) {
		default:
			return false
		}
	}

	AuroraAdmin_PruneTasks_Helper.WrapResponse = func(success *Response, err error) (*AuroraAdmin_PruneTasks_Result, error) {
		if err == nil {
			return &AuroraAdmin_PruneTasks_Result{Success: success}, nil
		}

		return nil, err
	}
	AuroraAdmin_PruneTasks_Helper.UnwrapResponse = func(result *AuroraAdmin_PruneTasks_Result) (success *Response, err error) {

		if result.Success != nil {
			success = result.Success
			return
		}

		err = errors.New("expected a non-void result")
		return
	}

}

// AuroraAdmin_PruneTasks_Result represents the result of a AuroraAdmin.pruneTasks function call.
//
// The result of a pruneTasks execution is sent and received over the wire as this struct.
//
// Success is set only if the function did not throw an exception.
type AuroraAdmin_PruneTasks_Result struct {
	// Value returned by pruneTasks after a successful execution.
	Success *Response `json:"success,omitempty"`
}

// ToWire translates a AuroraAdmin_PruneTasks_Result struct into a Thrift-level intermediate
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
func (v *AuroraAdmin_PruneTasks_Result) ToWire() (wire.Value, error) {
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
		return wire.Value{}, fmt.Errorf("AuroraAdmin_PruneTasks_Result should have exactly one field: got %v fields", i)
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraAdmin_PruneTasks_Result struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraAdmin_PruneTasks_Result struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraAdmin_PruneTasks_Result
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraAdmin_PruneTasks_Result) FromWire(w wire.Value) error {
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
		return fmt.Errorf("AuroraAdmin_PruneTasks_Result should have exactly one field: got %v fields", count)
	}

	return nil
}

// String returns a readable string representation of a AuroraAdmin_PruneTasks_Result
// struct.
func (v *AuroraAdmin_PruneTasks_Result) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Success != nil {
		fields[i] = fmt.Sprintf("Success: %v", v.Success)
		i++
	}

	return fmt.Sprintf("AuroraAdmin_PruneTasks_Result{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraAdmin_PruneTasks_Result match the
// provided AuroraAdmin_PruneTasks_Result.
//
// This function performs a deep comparison.
func (v *AuroraAdmin_PruneTasks_Result) Equals(rhs *AuroraAdmin_PruneTasks_Result) bool {
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
// fast logging of AuroraAdmin_PruneTasks_Result.
func (v *AuroraAdmin_PruneTasks_Result) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
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
func (v *AuroraAdmin_PruneTasks_Result) GetSuccess() (o *Response) {
	if v != nil && v.Success != nil {
		return v.Success
	}

	return
}

// IsSetSuccess returns true if Success is not nil.
func (v *AuroraAdmin_PruneTasks_Result) IsSetSuccess() bool {
	return v != nil && v.Success != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the result.
//
// This will always be "pruneTasks" for this struct.
func (v *AuroraAdmin_PruneTasks_Result) MethodName() string {
	return "pruneTasks"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Reply for this struct.
func (v *AuroraAdmin_PruneTasks_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
