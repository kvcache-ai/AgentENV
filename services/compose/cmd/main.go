package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"agentenv/services/compose"
)

// exitInvalid (EX_DATAERR) marks a Compose project the sandbox cannot run.
// Go exits with status 2 on panics and fatal runtime errors, so status 2 must
// not be mistaken for invalid user input. Every other failure exits with 1.
const exitInvalid = 65

func main() {
	defer func() {
		if r := recover(); r != nil {
			exit(1, fmt.Errorf("compose planner panicked: %v", r))
		}
	}()
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	var request compose.Request
	if err := decoder.Decode(&request); err != nil {
		exit(1, fmt.Errorf("decode planner request: %w", err))
	}
	plan, err := compose.Prepare(context.Background(), request)
	if err != nil {
		var invalid *compose.InvalidError
		if errors.As(err, &invalid) {
			exit(exitInvalid, err)
		}
		exit(1, err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(plan); err != nil {
		exit(1, err)
	}
}

func exit(code int, err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(code)
}
