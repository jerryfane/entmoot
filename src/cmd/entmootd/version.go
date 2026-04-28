package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

type versionOutput struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

func cmdVersion(_ *globalFlags, args []string) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd version")
	}
	if err := fs.Parse(args); err != nil {
		return exitInvalidArgument
	}
	if fs.NArg() != 0 {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "entmootd version: unexpected arguments")
		return exitInvalidArgument
	}

	if err := emitJSON(versionOutput{
		Version: version,
		Commit:  commit,
		Date:    date,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "entmootd version: %v\n", err)
		return exitTransport
	}
	return exitOK
}
