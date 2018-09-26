package buildversion

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionHandler(t *testing.T) {
	var handlerTests = []struct {
		expectedCode    int
		containResponse string
	}{
		{
			expectedCode:    http.StatusOK,
			containResponse: "0.7.5-52-gebfbd1b4",
		},
	}

	for _, tt := range handlerTests {
		handler := Handler("0.7.5-52-gebfbd1b4")
		req := httptest.NewRequest("GET", "http://example.com/version", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Contains(t, string(body), tt.containResponse)
		assert.Equal(t, tt.expectedCode, resp.StatusCode)
	}

}
