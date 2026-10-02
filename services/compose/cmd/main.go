package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"agentenv/services/compose"
)

func main() {
	request, err := readRequest(os.Stdin)
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

func readRequest(reader io.Reader) (compose.Request, error) {
	var request compose.Request
	input, err := io.ReadAll(io.LimitReader(reader, compose.MaxRequestBytes+1))
	if err != nil {
		return request, err
	}
	if len(input) > compose.MaxRequestBytes {
		return request, fmt.Errorf("Compose planner request exceeds %d bytes", compose.MaxRequestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return request, fmt.Errorf("expected one JSON request")
	}
	return request, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
