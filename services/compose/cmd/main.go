package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"agentenv/services/compose"
)

func main() {
	var request compose.Request
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		fail(err)
	}
	plan, err := compose.Prepare(context.Background(), request)
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
