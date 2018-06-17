package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"

	"io/ioutil"
	"net/http"
	"net/http/httptest"
)

func TestLevelOverwriteHandler(t *testing.T) {
	var handlerTests = []struct {
		url              string
		expectedCode     int
		containResponse  string
		expectedLogLevel int
	}{
		{
			url:             "",
			expectedCode:    http.StatusBadRequest,
			containResponse: "Required params not set:",
		},
		{
			url:             "?duration=3s",
			expectedCode:    http.StatusBadRequest,
			containResponse: "Required params not set:",
		},
		{
			url:             "?level=info",
			expectedCode:    http.StatusBadRequest,
			containResponse: "Required params not set:",
		},
		{
			url:             "?duration=3s",
			expectedCode:    http.StatusBadRequest,
			containResponse: "Required params not set:",
		},
		{
			url:              "?level=debug&duration=3s",
			expectedCode:     http.StatusOK,
			containResponse:  "Level changed to debug",
			expectedLogLevel: int(log.DebugLevel),
		},
		{
			url:             "?level=warn&duration=3s",
			expectedCode:    http.StatusBadRequest,
			containResponse: "New Level warn is not info or debug",
		},
		{
			url:             "?level=debug&duration=time",
			expectedCode:    http.StatusBadRequest,
			containResponse: "invalid duration time",
		},
		{
			url:             "?level=log&duration=3s",
			expectedCode:    http.StatusBadRequest,
			containResponse: "not a valid logrus Level",
		},
	}

	for _, tt := range handlerTests {
		handler := LevelOverwriteHandler(log.InfoLevel)
		req := httptest.NewRequest("GET", "http://example.com/path"+tt.url, nil)
		w := httptest.NewRecorder()
		handler(w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Contains(t, string(body), tt.containResponse)
		assert.Equal(t, tt.expectedCode, resp.StatusCode)

		if tt.expectedLogLevel != 0 {
			assert.Equal(t, log.Level(tt.expectedLogLevel), log.GetLevel())
		}
	}

}
