package mpb

import (
	"fmt"
	"reflect"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
)

var (
	_reqMetaType        = reflect.TypeOf((*yarpc.ReqMeta)(nil)).Elem()
	_errorType          = reflect.TypeOf((*error)(nil)).Elem()
	_interfaceEmptyType = reflect.TypeOf((*interface{})(nil)).Elem()
)

// Registrant is used for types that define or know about different JSON
// procedures.
type Registrant interface {
	// Gets a mapping from procedure name to the handler for that procedure for
	// all procedures provided by this registrant.
	getHandlers() map[string]interface{}
}

// procedure is a simple Registrant that has a single procedure.
type procedure struct {
	Name    string
	Handler interface{}
}

func (p procedure) getHandlers() map[string]interface{} {
	return map[string]interface{}{p.Name: p.Handler}
}

// Procedure builds a Registrant with a single procedure in it. handler must
// be a function with a signature similar to,
//
// 	f(reqMeta yarpc.ReqMeta, body $reqBody) ($resBody, yarpc.ResMeta, error)
//
// Where $reqBody and $resBody are a map[string]interface{} or pointers to
// structs.
func Procedure(name string, handler interface{}) Registrant {
	return procedure{Name: name, Handler: handler}
}

// Register registers the procedures defined by the given JSON registrant with
// the given registry.
//
// Handlers must have a signature similar to the following or the system will
// panic.
//
// 	f(reqMeta yarpc.ReqMeta, body $reqBody) ($resBody, yarpc.ResMeta, error)
//
// Where $reqBody and $resBody are a map[string]interface{} or pointers to
// structs.
func Register(reg transport.Registrar, svc string, registrant Registrant) {
	for name, handler := range registrant.getHandlers() {
		verifySignature(name, reflect.TypeOf(handler))
		reg.Register([]transport.Registrant{
			{
				Service:     svc,
				Procedure:   name,
				HandlerSpec: transport.NewUnaryHandlerSpec(mpbHandler{handler: reflect.ValueOf(handler)}),
			}})
	}
}

// verifySignature verifies that the given type matches what we expect from
// Mesos json handlers
//
// Returns the request type.
func verifySignature(n string, t reflect.Type) {
	if t.Kind() != reflect.Func {
		panic(fmt.Sprintf(
			"handler for %q is not a function but a %v", n, t.Kind(),
		))
	}

	if t.NumIn() != 2 {
		panic(fmt.Sprintf(
			"expected handler for %q to have 2 arguments but it had %v",
			n, t.NumIn(),
		))
	}

	if t.NumOut() != 1 {
		panic(fmt.Sprintf(
			"expected handler for %q to have 1 results but it had %v",
			n, t.NumOut(),
		))
	}

	if t.In(0) != _reqMetaType {
		panic(fmt.Sprintf(
			"the first argument of the handler for %q must be of type "+
				"yarpc.ReqMeta, and not: %v", n, t.In(0),
		))
	}

	if t.Out(0) != _errorType {
		panic(fmt.Sprintf(
			"the last resultsof the handler for %q must be of type error, "+
				"and not: %v",
			n, t.Out(0),
		))
	}

	reqBodyType := t.In(1)

	if !isValidReqResType(reqBodyType) {
		panic(fmt.Sprintf(
			"the second argument of the handler for %q must be "+
				"a struct pointer, or interface{}, and not: %v",
			n, reqBodyType,
		))
	}

}

// isValidReqResType checks if the given type is a pointer to a
// struct or a interface{}.
func isValidReqResType(t reflect.Type) bool {
	return (t == _interfaceEmptyType) ||
		(t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct)
}
