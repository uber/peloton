package cli

import (
	ej "encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
)

var (
	tabWriter = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)
)

func printResponseJSON(response interface{}) {
	buffer, err := ej.MarshalIndent(response, "", "  ")
	if err == nil {
		fmt.Printf("%v\n", string(buffer))
	} else {
		fmt.Printf("MarshalIndent err=%v\n", err)
	}
}
