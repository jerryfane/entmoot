package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"entmoot/pkg/entmoot/esphttp"
)

func cmdFleet(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "fleet: missing op (want: list, info, activity)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "list":
		return cmdFleetList(gf, args[1:])
	case "info":
		return cmdFleetInfo(gf, args[1:])
	case "activity":
		return cmdFleetActivity(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fleet: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdFleetList(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet list", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		slog.Error("fleet list: open state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer state.Close()
	fleets, err := state.ListFleets(withBackgroundTimeout())
	if err != nil {
		slog.Error("fleet list: list", slog.String("err", err.Error()))
		return exitTransport
	}
	return printJSON(map[string]any{"fleets": esphttp.ActiveFleetRecords(fleets)})
}

func cmdFleetInfo(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet info", flag.ContinueOnError)
	fleetID := fs.String("fleet", "", "fleet id")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *fleetID == "" {
		fmt.Fprintln(os.Stderr, "fleet info: -fleet is required")
		return exitInvalidArgument
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		slog.Error("fleet info: open state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer state.Close()
	ctx := withBackgroundTimeout()
	fleet, ok, err := state.GetFleet(ctx, *fleetID)
	if err != nil {
		slog.Error("fleet info: get", slog.String("err", err.Error()))
		return exitTransport
	}
	if !ok {
		fmt.Fprintf(os.Stderr, "fleet info: fleet %s not found\n", *fleetID)
		return exitGroupNotFound
	}
	members, err := state.ListFleetMembers(ctx, *fleetID)
	if err != nil {
		slog.Error("fleet info: members", slog.String("err", err.Error()))
		return exitTransport
	}
	invites, err := state.ListFleetInvites(ctx, *fleetID)
	if err != nil {
		slog.Error("fleet info: invites", slog.String("err", err.Error()))
		return exitTransport
	}
	return printJSON(map[string]any{"fleet": fleet, "members": members, "invites": invites})
}

func cmdFleetActivity(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet activity", flag.ContinueOnError)
	fleetID := fs.String("fleet", "", "fleet id")
	limit := fs.Int("limit", 50, "maximum events")
	before := fs.String("before-ms", "", "exclusive upper created_at_ms bound")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *fleetID == "" {
		fmt.Fprintln(os.Stderr, "fleet activity: -fleet is required")
		return exitInvalidArgument
	}
	var beforeMS int64
	if *before != "" {
		v, err := strconv.ParseInt(*before, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fleet activity: -before-ms: %v\n", err)
			return exitInvalidArgument
		}
		beforeMS = v
	}
	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		slog.Error("fleet activity: open state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer state.Close()
	activity, err := state.ListFleetActivity(withBackgroundTimeout(), *fleetID, *limit, beforeMS)
	if err != nil {
		slog.Error("fleet activity: list", slog.String("err", err.Error()))
		return exitTransport
	}
	return printJSON(map[string]any{"activity": activity})
}

func withBackgroundTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx
}

func printJSON(v any) int {
	data, err := json.Marshal(v)
	if err != nil {
		slog.Error("json marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}
