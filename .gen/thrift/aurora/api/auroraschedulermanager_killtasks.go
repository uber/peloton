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

// AuroraSchedulerManager_KillTasks_Args represents the arguments for the AuroraSchedulerManager.killTasks function.
//
// The arguments for killTasks are sent and received over the wire as this struct.
type AuroraSchedulerManager_KillTasks_Args struct {
	Job       *JobKey            `json:"job,omitempty"`
	Instances map[int32]struct{} `json:"instances,omitempty"`
	Message   *string            `json:"message,omitempty"`
}

// ToWire translates a AuroraSchedulerManager_KillTasks_Args struct into a Thrift-level intermediate
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
func (v *AuroraSchedulerManager_KillTasks_Args) ToWire() (wire.Value, error) {
	var (
		fields [3]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.Job != nil {
		w, err = v.Job.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 4, Value: w}
		i++
	}
	if v.Instances != nil {
		w, err = wire.NewValueSet(_Set_I32_mapType_ValueList(v.Instances)), error(nil)
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 5, Value: w}
		i++
	}
	if v.Message != nil {
		w, err = wire.NewValueString(*(v.Message)), error(nil)
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 6, Value: w}
		i++
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraSchedulerManager_KillTasks_Args struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraSchedulerManager_KillTasks_Args struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraSchedulerManager_KillTasks_Args
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraSchedulerManager_KillTasks_Args) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 4:
			if field.Value.Type() == wire.TStruct {
				v.Job, err = _JobKey_Read(field.Value)
				if err != nil {
					return err
				}

			}
		case 5:
			if field.Value.Type() == wire.TSet {
				v.Instances, err = _Set_I32_mapType_Read(field.Value.GetSet())
				if err != nil {
					return err
				}

			}
		case 6:
			if field.Value.Type() == wire.TBinary {
				var x string
				x, err = field.Value.GetString(), error(nil)
				v.Message = &x
				if err != nil {
					return err
				}

			}
		}
	}

	return nil
}

// String returns a readable string representation of a AuroraSchedulerManager_KillTasks_Args
// struct.
func (v *AuroraSchedulerManager_KillTasks_Args) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [3]string
	i := 0
	if v.Job != nil {
		fields[i] = fmt.Sprintf("Job: %v", v.Job)
		i++
	}
	if v.Instances != nil {
		fields[i] = fmt.Sprintf("Instances: %v", v.Instances)
		i++
	}
	if v.Message != nil {
		fields[i] = fmt.Sprintf("Message: %v", *(v.Message))
		i++
	}

	return fmt.Sprintf("AuroraSchedulerManager_KillTasks_Args{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraSchedulerManager_KillTasks_Args match the
// provided AuroraSchedulerManager_KillTasks_Args.
//
// This function performs a deep comparison.
func (v *AuroraSchedulerManager_KillTasks_Args) Equals(rhs *AuroraSchedulerManager_KillTasks_Args) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !((v.Job == nil && rhs.Job == nil) || (v.Job != nil && rhs.Job != nil && v.Job.Equals(rhs.Job))) {
		return false
	}
	if !((v.Instances == nil && rhs.Instances == nil) || (v.Instances != nil && rhs.Instances != nil && _Set_I32_mapType_Equals(v.Instances, rhs.Instances))) {
		return false
	}
	if !_String_EqualsPtr(v.Message, rhs.Message) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of AuroraSchedulerManager_KillTasks_Args.
func (v *AuroraSchedulerManager_KillTasks_Args) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.Job != nil {
		err = multierr.Append(err, enc.AddObject("job", v.Job))
	}
	if v.Instances != nil {
		err = multierr.Append(err, enc.AddArray("instances", (_Set_I32_mapType_Zapper)(v.Instances)))
	}
	if v.Message != nil {
		enc.AddString("message", *v.Message)
	}
	return err
}

// GetJob returns the value of Job if it is set or its
// zero value if it is unset.
func (v *AuroraSchedulerManager_KillTasks_Args) GetJob() (o *JobKey) {
	if v != nil && v.Job != nil {
		return v.Job
	}

	return
}

// IsSetJob returns true if Job is not nil.
func (v *AuroraSchedulerManager_KillTasks_Args) IsSetJob() bool {
	return v != nil && v.Job != nil
}

// GetInstances returns the value of Instances if it is set or its
// zero value if it is unset.
func (v *AuroraSchedulerManager_KillTasks_Args) GetInstances() (o map[int32]struct{}) {
	if v != nil && v.Instances != nil {
		return v.Instances
	}

	return
}

// IsSetInstances returns true if Instances is not nil.
func (v *AuroraSchedulerManager_KillTasks_Args) IsSetInstances() bool {
	return v != nil && v.Instances != nil
}

// GetMessage returns the value of Message if it is set or its
// zero value if it is unset.
func (v *AuroraSchedulerManager_KillTasks_Args) GetMessage() (o string) {
	if v != nil && v.Message != nil {
		return *v.Message
	}

	return
}

// IsSetMessage returns true if Message is not nil.
func (v *AuroraSchedulerManager_KillTasks_Args) IsSetMessage() bool {
	return v != nil && v.Message != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the arguments.
//
// This will always be "killTasks" for this struct.
func (v *AuroraSchedulerManager_KillTasks_Args) MethodName() string {
	return "killTasks"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Call for this struct.
func (v *AuroraSchedulerManager_KillTasks_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

// AuroraSchedulerManager_KillTasks_Helper provides functions that aid in handling the
// parameters and return values of the AuroraSchedulerManager.killTasks
// function.
var AuroraSchedulerManager_KillTasks_Helper = struct {
	// Args accepts the parameters of killTasks in-order and returns
	// the arguments struct for the function.
	Args func(
		job *JobKey,
		instances map[int32]struct{},
		message *string,
	) *AuroraSchedulerManager_KillTasks_Args

	// IsException returns true if the given error can be thrown
	// by killTasks.
	//
	// An error can be thrown by killTasks only if the
	// corresponding exception type was mentioned in the 'throws'
	// section for it in the Thrift file.
	IsException func(error) bool

	// WrapResponse returns the result struct for killTasks
	// given its return value and error.
	//
	// This allows mapping values and errors returned by
	// killTasks into a serializable result struct.
	// WrapResponse returns a non-nil error if the provided
	// error cannot be thrown by killTasks
	//
	//   value, err := killTasks(args)
	//   result, err := AuroraSchedulerManager_KillTasks_Helper.WrapResponse(value, err)
	//   if err != nil {
	//     return fmt.Errorf("unexpected error from killTasks: %v", err)
	//   }
	//   serialize(result)
	WrapResponse func(*Response, error) (*AuroraSchedulerManager_KillTasks_Result, error)

	// UnwrapResponse takes the result struct for killTasks
	// and returns the value or error returned by it.
	//
	// The error is non-nil only if killTasks threw an
	// exception.
	//
	//   result := deserialize(bytes)
	//   value, err := AuroraSchedulerManager_KillTasks_Helper.UnwrapResponse(result)
	UnwrapResponse func(*AuroraSchedulerManager_KillTasks_Result) (*Response, error)
}{}

func init() {
	AuroraSchedulerManager_KillTasks_Helper.Args = func(
		job *JobKey,
		instances map[int32]struct{},
		message *string,
	) *AuroraSchedulerManager_KillTasks_Args {
		return &AuroraSchedulerManager_KillTasks_Args{
			Job:       job,
			Instances: instances,
			Message:   message,
		}
	}

	AuroraSchedulerManager_KillTasks_Helper.IsException = func(err error) bool {
		switch err.(type) {
		default:
			return false
		}
	}

	AuroraSchedulerManager_KillTasks_Helper.WrapResponse = func(success *Response, err error) (*AuroraSchedulerManager_KillTasks_Result, error) {
		if err == nil {
			return &AuroraSchedulerManager_KillTasks_Result{Success: success}, nil
		}

		return nil, err
	}
	AuroraSchedulerManager_KillTasks_Helper.UnwrapResponse = func(result *AuroraSchedulerManager_KillTasks_Result) (success *Response, err error) {

		if result.Success != nil {
			success = result.Success
			return
		}

		err = errors.New("expected a non-void result")
		return
	}

}

// AuroraSchedulerManager_KillTasks_Result represents the result of a AuroraSchedulerManager.killTasks function call.
//
// The result of a killTasks execution is sent and received over the wire as this struct.
//
// Success is set only if the function did not throw an exception.
type AuroraSchedulerManager_KillTasks_Result struct {
	// Value returned by killTasks after a successful execution.
	Success *Response `json:"success,omitempty"`
}

// ToWire translates a AuroraSchedulerManager_KillTasks_Result struct into a Thrift-level intermediate
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
func (v *AuroraSchedulerManager_KillTasks_Result) ToWire() (wire.Value, error) {
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
		return wire.Value{}, fmt.Errorf("AuroraSchedulerManager_KillTasks_Result should have exactly one field: got %v fields", i)
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraSchedulerManager_KillTasks_Result struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraSchedulerManager_KillTasks_Result struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraSchedulerManager_KillTasks_Result
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraSchedulerManager_KillTasks_Result) FromWire(w wire.Value) error {
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
		return fmt.Errorf("AuroraSchedulerManager_KillTasks_Result should have exactly one field: got %v fields", count)
	}

	return nil
}

// String returns a readable string representation of a AuroraSchedulerManager_KillTasks_Result
// struct.
func (v *AuroraSchedulerManager_KillTasks_Result) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Success != nil {
		fields[i] = fmt.Sprintf("Success: %v", v.Success)
		i++
	}

	return fmt.Sprintf("AuroraSchedulerManager_KillTasks_Result{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraSchedulerManager_KillTasks_Result match the
// provided AuroraSchedulerManager_KillTasks_Result.
//
// This function performs a deep comparison.
func (v *AuroraSchedulerManager_KillTasks_Result) Equals(rhs *AuroraSchedulerManager_KillTasks_Result) bool {
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
// fast logging of AuroraSchedulerManager_KillTasks_Result.
func (v *AuroraSchedulerManager_KillTasks_Result) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
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
func (v *AuroraSchedulerManager_KillTasks_Result) GetSuccess() (o *Response) {
	if v != nil && v.Success != nil {
		return v.Success
	}

	return
}

// IsSetSuccess returns true if Success is not nil.
func (v *AuroraSchedulerManager_KillTasks_Result) IsSetSuccess() bool {
	return v != nil && v.Success != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the result.
//
// This will always be "killTasks" for this struct.
func (v *AuroraSchedulerManager_KillTasks_Result) MethodName() string {
	return "killTasks"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Reply for this struct.
func (v *AuroraSchedulerManager_KillTasks_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
