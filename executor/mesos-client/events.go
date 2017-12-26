package client

import (
	"bufio"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
)

// EventHandler is the handler interface that will receive the events coming into the
// SUBSCRIBE tcp connection.
type EventHandler interface {
	Connected()
	HandleEvent(*executor.Event) error
}

// JSONUnmarshal is an unmarshalling method from json into a protobuf message.
func JSONUnmarshal(content []byte, msg proto.Message) error { return json.Unmarshal(content, msg) }

// DispatchEvents reads through a chunked response, parses the events and calls the
// HandleEvent function on each event it comes accross. If the HandleEvent function returns
// an error, the connection is severed and disconnected is called.
func DispatchEvents(body io.ReadCloser, unmarshal UnmarshalFunc, handler EventHandler) error {
	defer body.Close()

	reader := bufio.NewReader(body)
	for {
		// Read chunk length
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return errors.Wrapf(err, "Failed to read chunk line")
		} else if err == io.EOF {
			return errors.Wrapf(err, "Received unexpected EOF")
		}
		line = strings.Trim(line, "\n")

		// Parse it to integer
		length, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "Failed to parse chunk size from string '%s'", line)
		} else if length == 0 {
			log.Infof("Received 0-chunk, closing connection")
			return nil
		}

		// Read the chunk data
		buffer := make([]byte, length)
		_, err = io.ReadFull(reader, buffer)
		if err != nil {
			return errors.Wrapf(err, "Failed to read chunk data")
		}

		// Parse it to event
		event := &executor.Event{}
		err = unmarshal(buffer, event)
		if err != nil {
			return errors.Wrapf(err, "Failed to parse event data")
		}

		// Give it to the handler
		if err := handler.HandleEvent(event); err != nil {
			return errors.Wrapf(err, "Handler failed to handle event")
		}
	}
}
