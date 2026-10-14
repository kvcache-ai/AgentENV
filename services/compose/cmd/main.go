package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"agentenv/services/compose"
)

func main() {
	request, err := compose.ReadRequest(os.Stdin)
	if err != nil {
		fail(err)
	}
	var plan any
	switch request.Mode {
	case "":
		plan, err = compose.Prepare(context.Background(), request)
	case "build":
		plan, err = compose.PrepareBuild(context.Background(), request)
	default:
		err = fmt.Errorf("unknown planner mode %q", request.Mode)
	}
	if err != nil {
		fail(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(plan); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
