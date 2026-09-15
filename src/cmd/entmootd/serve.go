package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
)

var (
	errServeNoGroups       = errors.New("no joined groups found; run entmootd join <invite> once")
	errServeGroupMissing   = errors.New("joined group not found")
	errServeInvalidGroupID = errors.New("invalid group id")
)

func cmdServe(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var groups repeatedStringFlag
	fs.Var(&groups, "group", "base64 group id to serve; may be repeated (default: all joined groups)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "serve: unexpected argument %q\n", fs.Arg(0))
		return exitInvalidArgument
	}
	for _, raw := range groups {
		if _, err := decodeGroupID(raw); err != nil {
			fmt.Fprintf(os.Stderr, "serve: invalid -group: %v\n", err)
			return exitInvalidArgument
		}
	}
	selectedGroups, err := selectServeGroupIDs(gf.data, []string(groups), slog.Default())
	if err != nil {
		switch {
		case errors.Is(err, errServeInvalidGroupID):
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			return exitInvalidArgument
		case errors.Is(err, errServeNoGroups), errors.Is(err, errServeGroupMissing):
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			return exitGroupNotFound
		default:
			slog.Error("serve: select groups", slog.String("err", err.Error()))
			return exitTransport
		}
	}

	return runGroupDaemon(gf, groupDaemonOptions{
		command: "serve",
		event:   "serving",
		loadGroups: func(ctx context.Context, runtime *groupRuntime, _ groupDaemonLoadContext) (int, error) {
			strict := len(groups) > 0
			// A group carried over from the linear chain has no checkpoint
			// until its founder mints one, and only the founder can. Take it
			// from a peer first, and keep trying in the background: an
			// operator who upgrades the founder should not have to restart
			// every other node afterwards.
			runtime.adoptPendingGroups(ctx, selectedGroups)
			runtime.retryPendingAdoptions(ctx, selectedGroups)
			for _, gid := range selectedGroups {
				if !membershipExists(gf.data, gid) {
					continue
				}
				if _, _, err := runtime.AddLocalGroup(ctx, gid); err != nil {
					switch {
					case errors.Is(err, errLocalGroupNotMember), errors.Is(err, errLocalGroupIdentityMismatch):
						if strict {
							return exitNotMember, fmt.Errorf("group %s: %w", gid.String(), err)
						}
						slog.Warn("serve: skipping group that does not match local identity",
							slog.String("group_id", gid.String()),
							slog.String("err", err.Error()))
						continue
					default:
						return exitTransport, fmt.Errorf("start group %s: %w", gid.String(), err)
					}
				}
			}
			if runtime.Count() == 0 {
				return exitGroupNotFound, errServeNoGroups
			}
			return exitOK, nil
		},
	})
}

func selectServeGroupIDs(dataRoot string, selected []string, logger *slog.Logger) ([]entmoot.GroupID, error) {
	declinedDefaultMoot, hasDeclinedDefaultMoot := defaultMootDeclinedGroupID(dataRoot)
	if len(selected) > 0 {
		out := make([]entmoot.GroupID, 0, len(selected))
		seen := make(map[entmoot.GroupID]struct{}, len(selected))
		for _, raw := range selected {
			gid, err := decodeGroupID(raw)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", errServeInvalidGroupID, err)
			}
			if hasDeclinedDefaultMoot && gid == declinedDefaultMoot {
				if logger != nil {
					logger.Warn("serve: skipping declined default moot",
						slog.String("group_id", gid.String()))
				}
				continue
			}
			if _, ok := seen[gid]; ok {
				continue
			}
			if !membershipExists(dataRoot, gid) && !membership.LegacyExists(dataRoot, gid) {
				return nil, fmt.Errorf("%w: %s", errServeGroupMissing, gid.String())
			}
			seen[gid] = struct{}{}
			out = append(out, gid)
		}
		if len(out) == 0 {
			return nil, errServeNoGroups
		}
		return out, nil
	}

	gids, err := listGroupIDs(dataRoot, logger)
	if err != nil {
		return nil, err
	}
	out := make([]entmoot.GroupID, 0, len(gids))
	for _, gid := range gids {
		if hasDeclinedDefaultMoot && gid == declinedDefaultMoot {
			if logger != nil {
				logger.Warn("serve: skipping declined default moot",
					slog.String("group_id", gid.String()))
			}
			continue
		}
		if !membershipExists(dataRoot, gid) && !membership.LegacyExists(dataRoot, gid) {
			if logger != nil {
				logger.Warn("serve: skipping group with no membership state",
					slog.String("group_id", gid.String()))
			}
			continue
		}
		out = append(out, gid)
	}
	if len(out) == 0 {
		return nil, errServeNoGroups
	}
	return out, nil
}

func membershipExists(dataRoot string, gid entmoot.GroupID) bool {
	return groupMembershipExists(dataRoot, gid)
}
