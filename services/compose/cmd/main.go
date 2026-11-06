package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"agentenv/services/compose"
)

// exitInvalid (EX_DATAERR) marks a Compose project the sandbox cannot run.
// Go exits with status 2 on panics and fatal runtime errors, so status 2 must
// not be mistaken for invalid user input. Every other failure exits with 1.
const exitInvalid = 65

type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			exit(1, fmt.Errorf("compose planner panicked: %v", r))
		}
	}()
	var options compose.Options
	flags := flag.NewFlagSet("aenv-compose-plan", flag.ContinueOnError)
	flags.Var((*stringList)(&options.ConfigPaths), "f", "Compose configuration file (repeatable; - reads stdin)")
	flags.StringVar(&options.ProjectDir, "project-directory", "", "Alternate working directory")
	flags.Var((*stringList)(&options.EnvFiles), "env-file", "Alternate environment file (repeatable)")
	flags.StringVar(&options.ProjectName, "p", "", "Project name")
	flags.Var((*stringList)(&options.Profiles), "profile", "Profile to enable (repeatable)")
	envdPort := flags.Uint("envd-port", compose.DefaultEnvdPort, "Guest envd port, which services must not publish")
	flags.BoolVar(&options.Build, "build", false, "Plan only the service image builds, without the sandbox runtime checks")
	if err := flags.Parse(os.Args[1:]); err != nil {
		exit(1, err)
	}
	if flags.NArg() > 0 || *envdPort == 0 || *envdPort > 65535 {
		exit(1, fmt.Errorf("usage: aenv-compose-plan [-f FILE]... [-project-directory DIR] [-env-file FILE]... [-p NAME] [-profile NAME]... [-envd-port PORT] [-build]"))
	}
	options.EnvdPort = uint16(*envdPort)
	plan, err := compose.Prepare(context.Background(), options)
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
