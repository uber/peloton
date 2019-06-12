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

// AuroraSchedulerManager_RestartShards_Args represents the arguments for the AuroraSchedulerManager.restartShards function.
//
// The arguments for restartShards are sent and received over the wire as this struct.
type AuroraSchedulerManager_RestartShards_Args struct {
	Job      *JobKey            `json:"job,omitempty"`
	ShardIds map[int32]struct{} `json:"shardIds,omitempty"`
}

// ToWire translates a AuroraSchedulerManager_RestartShards_Args struct into a Thrift-level intermediate
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
func (v *AuroraSchedulerManager_RestartShards_Args) ToWire() (wire.Value, error) {
	var (
		fields [2]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.Job != nil {
		w, err = v.Job.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 5, Value: w}
		i++
	}
	if v.ShardIds != nil {
		w, err = wire.NewValueSet(_Set_I32_mapType_ValueList(v.ShardIds)), error(nil)
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 3, Value: w}
		i++
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraSchedulerManager_RestartShards_Args struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraSchedulerManager_RestartShards_Args struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraSchedulerManager_RestartShards_Args
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraSchedulerManager_RestartShards_Args) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 5:
			if field.Value.Type() == wire.TStruct {
				v.Job, err = _JobKey_Read(field.Value)
				if err != nil {
					return err
				}

			}
		case 3:
			if field.Value.Type() == wire.TSet {
				v.ShardIds, err = _Set_I32_mapType_Read(field.Value.GetSet())
				if err != nil {
					return err
				}

			}
		}
	}

	return nil
}

// String returns a readable string representation of a AuroraSchedulerManager_RestartShards_Args
// struct.
func (v *AuroraSchedulerManager_RestartShards_Args) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [2]string
	i := 0
	if v.Job != nil {
		fields[i] = fmt.Sprintf("Job: %v", v.Job)
		i++
	}
	if v.ShardIds != nil {
		fields[i] = fmt.Sprintf("ShardIds: %v", v.ShardIds)
		i++
	}

	return fmt.Sprintf("AuroraSchedulerManager_RestartShards_Args{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraSchedulerManager_RestartShards_Args match the
// provided AuroraSchedulerManager_RestartShards_Args.
//
// This function performs a deep comparison.
func (v *AuroraSchedulerManager_RestartShards_Args) Equals(rhs *AuroraSchedulerManager_RestartShards_Args) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !((v.Job == nil && rhs.Job == nil) || (v.Job != nil && rhs.Job != nil && v.Job.Equals(rhs.Job))) {
		return false
	}
	if !((v.ShardIds == nil && rhs.ShardIds == nil) || (v.ShardIds != nil && rhs.ShardIds != nil && _Set_I32_mapType_Equals(v.ShardIds, rhs.ShardIds))) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of AuroraSchedulerManager_RestartShards_Args.
func (v *AuroraSchedulerManager_RestartShards_Args) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.Job != nil {
		err = multierr.Append(err, enc.AddObject("job", v.Job))
	}
	if v.ShardIds != nil {
		err = multierr.Append(err, enc.AddArray("shardIds", (_Set_I32_mapType_Zapper)(v.ShardIds)))
	}
	return err
}

// GetJob returns the value of Job if it is set or its
// zero value if it is unset.
func (v *AuroraSchedulerManager_RestartShards_Args) GetJob() (o *JobKey) {
	if v != nil && v.Job != nil {
		return v.Job
	}

	return
}

// IsSetJob returns true if Job is not nil.
func (v *AuroraSchedulerManager_RestartShards_Args) IsSetJob() bool {
	return v != nil && v.Job != nil
}

// GetShardIds returns the value of ShardIds if it is set or its
// zero value if it is unset.
func (v *AuroraSchedulerManager_RestartShards_Args) GetShardIds() (o map[int32]struct{}) {
	if v != nil && v.ShardIds != nil {
		return v.ShardIds
	}

	return
}

// IsSetShardIds returns true if ShardIds is not nil.
func (v *AuroraSchedulerManager_RestartShards_Args) IsSetShardIds() bool {
	return v != nil && v.ShardIds != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the arguments.
//
// This will always be "restartShards" for this struct.
func (v *AuroraSchedulerManager_RestartShards_Args) MethodName() string {
	return "restartShards"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Call for this struct.
func (v *AuroraSchedulerManager_RestartShards_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

// AuroraSchedulerManager_RestartShards_Helper provides functions that aid in handling the
// parameters and return values of the AuroraSchedulerManager.restartShards
// function.
var AuroraSchedulerManager_RestartShards_Helper = struct {
	// Args accepts the parameters of restartShards in-order and returns
	// the arguments struct for the function.
	Args func(
		job *JobKey,
		shardIds map[int32]struct{},
	) *AuroraSchedulerManager_RestartShards_Args

	// IsException returns true if the given error can be thrown
	// by restartShards.
	//
	// An error can be thrown by restartShards only if the
	// corresponding exception type was mentioned in the 'throws'
	// section for it in the Thrift file.
	IsException func(error) bool

	// WrapResponse returns the result struct for restartShards
	// given its return value and error.
	//
	// This allows mapping values and errors returned by
	// restartShards into a serializable result struct.
	// WrapResponse returns a non-nil error if the provided
	// error cannot be thrown by restartShards
	//
	//   value, err := restartShards(args)
	//   result, err := AuroraSchedulerManager_RestartShards_Helper.WrapResponse(value, err)
	//   if err != nil {
	//     return fmt.Errorf("unexpected error from restartShards: %v", err)
	//   }
	//   serialize(result)
	WrapResponse func(*Response, error) (*AuroraSchedulerManager_RestartShards_Result, error)

	// UnwrapResponse takes the result struct for restartShards
	// and returns the value or error returned by it.
	//
	// The error is non-nil only if restartShards threw an
	// exception.
	//
	//   result := deserialize(bytes)
	//   value, err := AuroraSchedulerManager_RestartShards_Helper.UnwrapResponse(result)
	UnwrapResponse func(*AuroraSchedulerManager_RestartShards_Result) (*Response, error)
}{}

func init() {
	AuroraSchedulerManager_RestartShards_Helper.Args = func(
		job *JobKey,
		shardIds map[int32]struct{},
	) *AuroraSchedulerManager_RestartShards_Args {
		return &AuroraSchedulerManager_RestartShards_Args{
			Job:      job,
			ShardIds: shardIds,
		}
	}

	AuroraSchedulerManager_RestartShards_Helper.IsException = func(err error) bool {
		switch err.(type) {
		default:
			return false
		}
	}

	AuroraSchedulerManager_RestartShards_Helper.WrapResponse = func(success *Response, err error) (*AuroraSchedulerManager_RestartShards_Result, error) {
		if err == nil {
			return &AuroraSchedulerManager_RestartShards_Result{Success: success}, nil
		}

		return nil, err
	}
	AuroraSchedulerManager_RestartShards_Helper.UnwrapResponse = func(result *AuroraSchedulerManager_RestartShards_Result) (success *Response, err error) {

		if result.Success != nil {
			success = result.Success
			return
		}

		err = errors.New("expected a non-void result")
		return
	}

}

// AuroraSchedulerManager_RestartShards_Result represents the result of a AuroraSchedulerManager.restartShards function call.
//
// The result of a restartShards execution is sent and received over the wire as this struct.
//
// Success is set only if the function did not throw an exception.
type AuroraSchedulerManager_RestartShards_Result struct {
	// Value returned by restartShards after a successful execution.
	Success *Response `json:"success,omitempty"`
}

// ToWire translates a AuroraSchedulerManager_RestartShards_Result struct into a Thrift-level intermediate
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
func (v *AuroraSchedulerManager_RestartShards_Result) ToWire() (wire.Value, error) {
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
		return wire.Value{}, fmt.Errorf("AuroraSchedulerManager_RestartShards_Result should have exactly one field: got %v fields", i)
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraSchedulerManager_RestartShards_Result struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraSchedulerManager_RestartShards_Result struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraSchedulerManager_RestartShards_Result
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraSchedulerManager_RestartShards_Result) FromWire(w wire.Value) error {
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
		return fmt.Errorf("AuroraSchedulerManager_RestartShards_Result should have exactly one field: got %v fields", count)
	}

	return nil
}

// String returns a readable string representation of a AuroraSchedulerManager_RestartShards_Result
// struct.
func (v *AuroraSchedulerManager_RestartShards_Result) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Success != nil {
		fields[i] = fmt.Sprintf("Success: %v", v.Success)
		i++
	}

	return fmt.Sprintf("AuroraSchedulerManager_RestartShards_Result{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraSchedulerManager_RestartShards_Result match the
// provided AuroraSchedulerManager_RestartShards_Result.
//
// This function performs a deep comparison.
func (v *AuroraSchedulerManager_RestartShards_Result) Equals(rhs *AuroraSchedulerManager_RestartShards_Result) bool {
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
// fast logging of AuroraSchedulerManager_RestartShards_Result.
func (v *AuroraSchedulerManager_RestartShards_Result) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
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
func (v *AuroraSchedulerManager_RestartShards_Result) GetSuccess() (o *Response) {
	if v != nil && v.Success != nil {
		return v.Success
	}

	return
}

// IsSetSuccess returns true if Success is not nil.
func (v *AuroraSchedulerManager_RestartShards_Result) IsSetSuccess() bool {
	return v != nil && v.Success != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the result.
//
// This will always be "restartShards" for this struct.
func (v *AuroraSchedulerManager_RestartShards_Result) MethodName() string {
	return "restartShards"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Reply for this struct.
func (v *AuroraSchedulerManager_RestartShards_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
