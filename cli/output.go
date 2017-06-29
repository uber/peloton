package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"

	"gopkg.in/yaml.v2"
)

var (
	tabWriter = tabwriter.NewWriter(
		os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug,
	)
)

func printResponseJSON(response interface{}) {
	buffer, err := json.MarshalIndent(response, "", "  ")
	if err == nil {
		fmt.Printf("%v\n", string(buffer))
	} else {
		fmt.Printf("MarshalIndent err=%v\n", err)
	}
}

// Marshall a pb message response in the desired format
func marshallResponse(
	format string,
	message proto.Message) ([]byte, error) {

	encoder := jsonpb.Marshaler{
		EnumsAsInts:  false,
		OrigName:     true,
		EmitDefaults: true,
	}

	// use jsonpb encoder to convert enum int into string
	body, err := encoder.MarshalToString(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal response : %v",
			err)
	}

	byt := []byte(body)
	var dat map[string]interface{}
	if err := json.Unmarshal(byt, &dat); err != nil {
		return nil, fmt.Errorf(
			"Failed to unmarshal response : %v",
			err)
	}

	switch strings.ToLower(format) {
	case "yaml", "yml":
		return yaml.Marshal(dat)
	case "json":
		return json.MarshalIndent(
			dat,
			"",
			"  ")
	default:
		return nil, fmt.Errorf("Invalid format %s", format)
	}
}
