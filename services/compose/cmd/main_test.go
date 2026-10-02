package main

import (
	"encoding/json"
	"strings"
	"testing"

	"agentenv/services/compose"
)

func TestRequestFrameBoundaries(t *testing.T) {
	valid := `{"compose":"services: {app: {image: busybox}}"}`
	for _, test := range []struct {
		name, input string
		wantErr     bool
	}{
		{"valid", valid, false},
		{"at limit", valid + strings.Repeat(" ", compose.MaxRequestBytes-len(valid)), false},
		{"over limit", valid + strings.Repeat(" ", compose.MaxRequestBytes-len(valid)+1), true},
		{"hidden second value", valid + strings.Repeat(" ", compose.MaxRequestBytes-len(valid)) + "{}", true},
		{"second value", valid + "{}", true},
		{"truncated", valid[:len(valid)-1], true},
		{"unknown field", `{"compose":"services: {}","unknown":true}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := readRequest(strings.NewReader(test.input))
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, test.wantErr)
			}
		})
	}
}

func TestHostDefaultsFitAfterMaximumHTTPRequest(t *testing.T) {
	request := map[string]any{
		"compose":    "services: {app: {image: busybox}}",
		"composeEnv": map[string]string{"P": ""},
	}
	empty, _ := json.Marshal(request)
	request["composeEnv"].(map[string]string)["P"] = strings.Repeat("x", 2*1024*1024-len(empty))
	request["profiles"] = []string{}
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(input) <= 2*1024*1024 {
		t.Fatal("fixture must exceed the old planner limit")
	}
	if _, err := readRequest(strings.NewReader(string(input))); err != nil {
		t.Fatal(err)
	}
}
