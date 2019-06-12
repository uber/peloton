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

// AuroraAdmin_SetQuota_Args represents the arguments for the AuroraAdmin.setQuota function.
//
// The arguments for setQuota are sent and received over the wire as this struct.
type AuroraAdmin_SetQuota_Args struct {
	OwnerRole *string            `json:"ownerRole,omitempty"`
	Quota     *ResourceAggregate `json:"quota,omitempty"`
}

// ToWire translates a AuroraAdmin_SetQuota_Args struct into a Thrift-level intermediate
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
func (v *AuroraAdmin_SetQuota_Args) ToWire() (wire.Value, error) {
	var (
		fields [2]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)

	if v.OwnerRole != nil {
		w, err = wire.NewValueString(*(v.OwnerRole)), error(nil)
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 1, Value: w}
		i++
	}
	if v.Quota != nil {
		w, err = v.Quota.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 2, Value: w}
		i++
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraAdmin_SetQuota_Args struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraAdmin_SetQuota_Args struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraAdmin_SetQuota_Args
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraAdmin_SetQuota_Args) FromWire(w wire.Value) error {
	var err error

	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 1:
			if field.Value.Type() == wire.TBinary {
				var x string
				x, err = field.Value.GetString(), error(nil)
				v.OwnerRole = &x
				if err != nil {
					return err
				}

			}
		case 2:
			if field.Value.Type() == wire.TStruct {
				v.Quota, err = _ResourceAggregate_Read(field.Value)
				if err != nil {
					return err
				}

			}
		}
	}

	return nil
}

// String returns a readable string representation of a AuroraAdmin_SetQuota_Args
// struct.
func (v *AuroraAdmin_SetQuota_Args) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [2]string
	i := 0
	if v.OwnerRole != nil {
		fields[i] = fmt.Sprintf("OwnerRole: %v", *(v.OwnerRole))
		i++
	}
	if v.Quota != nil {
		fields[i] = fmt.Sprintf("Quota: %v", v.Quota)
		i++
	}

	return fmt.Sprintf("AuroraAdmin_SetQuota_Args{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraAdmin_SetQuota_Args match the
// provided AuroraAdmin_SetQuota_Args.
//
// This function performs a deep comparison.
func (v *AuroraAdmin_SetQuota_Args) Equals(rhs *AuroraAdmin_SetQuota_Args) bool {
	if v == nil {
		return rhs == nil
	} else if rhs == nil {
		return false
	}
	if !_String_EqualsPtr(v.OwnerRole, rhs.OwnerRole) {
		return false
	}
	if !((v.Quota == nil && rhs.Quota == nil) || (v.Quota != nil && rhs.Quota != nil && v.Quota.Equals(rhs.Quota))) {
		return false
	}

	return true
}

// MarshalLogObject implements zapcore.ObjectMarshaler, enabling
// fast logging of AuroraAdmin_SetQuota_Args.
func (v *AuroraAdmin_SetQuota_Args) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	if v == nil {
		return nil
	}
	if v.OwnerRole != nil {
		enc.AddString("ownerRole", *v.OwnerRole)
	}
	if v.Quota != nil {
		err = multierr.Append(err, enc.AddObject("quota", v.Quota))
	}
	return err
}

// GetOwnerRole returns the value of OwnerRole if it is set or its
// zero value if it is unset.
func (v *AuroraAdmin_SetQuota_Args) GetOwnerRole() (o string) {
	if v != nil && v.OwnerRole != nil {
		return *v.OwnerRole
	}

	return
}

// IsSetOwnerRole returns true if OwnerRole is not nil.
func (v *AuroraAdmin_SetQuota_Args) IsSetOwnerRole() bool {
	return v != nil && v.OwnerRole != nil
}

// GetQuota returns the value of Quota if it is set or its
// zero value if it is unset.
func (v *AuroraAdmin_SetQuota_Args) GetQuota() (o *ResourceAggregate) {
	if v != nil && v.Quota != nil {
		return v.Quota
	}

	return
}

// IsSetQuota returns true if Quota is not nil.
func (v *AuroraAdmin_SetQuota_Args) IsSetQuota() bool {
	return v != nil && v.Quota != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the arguments.
//
// This will always be "setQuota" for this struct.
func (v *AuroraAdmin_SetQuota_Args) MethodName() string {
	return "setQuota"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Call for this struct.
func (v *AuroraAdmin_SetQuota_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

// AuroraAdmin_SetQuota_Helper provides functions that aid in handling the
// parameters and return values of the AuroraAdmin.setQuota
// function.
var AuroraAdmin_SetQuota_Helper = struct {
	// Args accepts the parameters of setQuota in-order and returns
	// the arguments struct for the function.
	Args func(
		ownerRole *string,
		quota *ResourceAggregate,
	) *AuroraAdmin_SetQuota_Args

	// IsException returns true if the given error can be thrown
	// by setQuota.
	//
	// An error can be thrown by setQuota only if the
	// corresponding exception type was mentioned in the 'throws'
	// section for it in the Thrift file.
	IsException func(error) bool

	// WrapResponse returns the result struct for setQuota
	// given its return value and error.
	//
	// This allows mapping values and errors returned by
	// setQuota into a serializable result struct.
	// WrapResponse returns a non-nil error if the provided
	// error cannot be thrown by setQuota
	//
	//   value, err := setQuota(args)
	//   result, err := AuroraAdmin_SetQuota_Helper.WrapResponse(value, err)
	//   if err != nil {
	//     return fmt.Errorf("unexpected error from setQuota: %v", err)
	//   }
	//   serialize(result)
	WrapResponse func(*Response, error) (*AuroraAdmin_SetQuota_Result, error)

	// UnwrapResponse takes the result struct for setQuota
	// and returns the value or error returned by it.
	//
	// The error is non-nil only if setQuota threw an
	// exception.
	//
	//   result := deserialize(bytes)
	//   value, err := AuroraAdmin_SetQuota_Helper.UnwrapResponse(result)
	UnwrapResponse func(*AuroraAdmin_SetQuota_Result) (*Response, error)
}{}

func init() {
	AuroraAdmin_SetQuota_Helper.Args = func(
		ownerRole *string,
		quota *ResourceAggregate,
	) *AuroraAdmin_SetQuota_Args {
		return &AuroraAdmin_SetQuota_Args{
			OwnerRole: ownerRole,
			Quota:     quota,
		}
	}

	AuroraAdmin_SetQuota_Helper.IsException = func(err error) bool {
		switch err.(type) {
		default:
			return false
		}
	}

	AuroraAdmin_SetQuota_Helper.WrapResponse = func(success *Response, err error) (*AuroraAdmin_SetQuota_Result, error) {
		if err == nil {
			return &AuroraAdmin_SetQuota_Result{Success: success}, nil
		}

		return nil, err
	}
	AuroraAdmin_SetQuota_Helper.UnwrapResponse = func(result *AuroraAdmin_SetQuota_Result) (success *Response, err error) {

		if result.Success != nil {
			success = result.Success
			return
		}

		err = errors.New("expected a non-void result")
		return
	}

}

// AuroraAdmin_SetQuota_Result represents the result of a AuroraAdmin.setQuota function call.
//
// The result of a setQuota execution is sent and received over the wire as this struct.
//
// Success is set only if the function did not throw an exception.
type AuroraAdmin_SetQuota_Result struct {
	// Value returned by setQuota after a successful execution.
	Success *Response `json:"success,omitempty"`
}

// ToWire translates a AuroraAdmin_SetQuota_Result struct into a Thrift-level intermediate
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
func (v *AuroraAdmin_SetQuota_Result) ToWire() (wire.Value, error) {
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
		return wire.Value{}, fmt.Errorf("AuroraAdmin_SetQuota_Result should have exactly one field: got %v fields", i)
	}

	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

// FromWire deserializes a AuroraAdmin_SetQuota_Result struct from its Thrift-level
// representation. The Thrift-level representation may be obtained
// from a ThriftRW protocol implementation.
//
// An error is returned if we were unable to build a AuroraAdmin_SetQuota_Result struct
// from the provided intermediate representation.
//
//   x, err := binaryProtocol.Decode(reader, wire.TStruct)
//   if err != nil {
//     return nil, err
//   }
//
//   var v AuroraAdmin_SetQuota_Result
//   if err := v.FromWire(x); err != nil {
//     return nil, err
//   }
//   return &v, nil
func (v *AuroraAdmin_SetQuota_Result) FromWire(w wire.Value) error {
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
		return fmt.Errorf("AuroraAdmin_SetQuota_Result should have exactly one field: got %v fields", count)
	}

	return nil
}

// String returns a readable string representation of a AuroraAdmin_SetQuota_Result
// struct.
func (v *AuroraAdmin_SetQuota_Result) String() string {
	if v == nil {
		return "<nil>"
	}

	var fields [1]string
	i := 0
	if v.Success != nil {
		fields[i] = fmt.Sprintf("Success: %v", v.Success)
		i++
	}

	return fmt.Sprintf("AuroraAdmin_SetQuota_Result{%v}", strings.Join(fields[:i], ", "))
}

// Equals returns true if all the fields of this AuroraAdmin_SetQuota_Result match the
// provided AuroraAdmin_SetQuota_Result.
//
// This function performs a deep comparison.
func (v *AuroraAdmin_SetQuota_Result) Equals(rhs *AuroraAdmin_SetQuota_Result) bool {
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
// fast logging of AuroraAdmin_SetQuota_Result.
func (v *AuroraAdmin_SetQuota_Result) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
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
func (v *AuroraAdmin_SetQuota_Result) GetSuccess() (o *Response) {
	if v != nil && v.Success != nil {
		return v.Success
	}

	return
}

// IsSetSuccess returns true if Success is not nil.
func (v *AuroraAdmin_SetQuota_Result) IsSetSuccess() bool {
	return v != nil && v.Success != nil
}

// MethodName returns the name of the Thrift function as specified in
// the IDL, for which this struct represent the result.
//
// This will always be "setQuota" for this struct.
func (v *AuroraAdmin_SetQuota_Result) MethodName() string {
	return "setQuota"
}

// EnvelopeType returns the kind of value inside this struct.
//
// This will always be Reply for this struct.
func (v *AuroraAdmin_SetQuota_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
