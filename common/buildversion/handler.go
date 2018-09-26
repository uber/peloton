package buildversion

import (
	"fmt"
	"net/http"
)

const (
	//Get is the default endpoint for getting peloton version.
	Get = "/version"
)

// Handler returns a handler for peloton version Get request
func Handler(version string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, version)
	}
}
