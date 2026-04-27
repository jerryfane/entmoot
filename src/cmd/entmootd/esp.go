package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/store"
)

func cmdESP(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "esp: expected serve")
		return exitInvalidArgument
	}
	switch args[0] {
	case "serve":
		return cmdESPServe(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "esp: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}

type espServeConfig struct {
	addr             string
	token            string
	allowNonLoopback bool
}

func cmdESPServe(gf *globalFlags, args []string) int {
	cfg, code, ok := parseESPServeConfig(args)
	if !ok {
		return code
	}
	if err := validateESPServeConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "esp serve: %v\n", err)
		return exitInvalidArgument
	}
	return runESPServe(gf, cfg)
}

func parseESPServeConfig(args []string) (espServeConfig, int, bool) {
	fs := flag.NewFlagSet("esp serve", flag.ContinueOnError)
	cfg := espServeConfig{}
	fs.StringVar(&cfg.addr, "addr", "127.0.0.1:8087", "HTTP listen address")
	fs.StringVar(&cfg.token, "token", "", "bearer token (defaults to ENTMOOT_ESP_TOKEN)")
	fs.BoolVar(&cfg.allowNonLoopback, "allow-non-loopback", false, "allow binding to a non-loopback interface")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cfg, exitOK, false
		}
		return cfg, exitInvalidArgument, false
	}
	if cfg.token == "" {
		cfg.token = os.Getenv("ENTMOOT_ESP_TOKEN")
	}
	return cfg, exitOK, true
}

func validateESPServeConfig(cfg espServeConfig) error {
	if cfg.token == "" {
		return errors.New("-token or ENTMOOT_ESP_TOKEN is required")
	}
	if !cfg.allowNonLoopback && !addrIsLoopback(cfg.addr) {
		return fmt.Errorf("-addr %s is not loopback; pass -allow-non-loopback only behind TLS/auth infrastructure", cfg.addr)
	}
	return nil
}

func runESPServe(gf *globalFlags, cfg espServeConfig) int {
	resources, err := openMailboxServiceResources(gf)
	if err != nil {
		slog.Error("esp serve: open mailbox resources", slog.String("err", err.Error()))
		return exitTransport
	}
	defer resources.close()

	handler, err := esphttp.NewHandler(esphttp.Config{
		Token:       cfg.token,
		Service:     resources.service,
		GroupExists: espGroupExists(gf.data),
		Logger:      slog.Default(),
	})
	if err != nil {
		slog.Error("esp serve: create handler", slog.String("err", err.Error()))
		return exitTransport
	}

	server := &http.Server{
		Addr:              cfg.addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("esp serve: listening", slog.String("addr", cfg.addr))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("esp serve: http server", slog.String("err", err.Error()))
		return exitTransport
	}
	return exitOK
}

type mailboxServiceResources struct {
	store       store.MessageStore
	cursorStore mailbox.CursorStore
	service     *mailbox.Service
}

func openMailboxServiceResources(gf *globalFlags) (*mailboxServiceResources, error) {
	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data: %w", err)
	}
	st, err := store.OpenSQLite(gf.data)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	cursors, err := mailbox.OpenSQLiteCursorStore(gf.data)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("open cursor store: %w", err)
	}
	svc, err := mailbox.NewWithCursorStore(st, cursors, nil)
	if err != nil {
		_ = cursors.Close()
		_ = st.Close()
		return nil, fmt.Errorf("create service: %w", err)
	}
	return &mailboxServiceResources{store: st, cursorStore: cursors, service: svc}, nil
}

func (r *mailboxServiceResources) close() {
	_ = r.cursorStore.Close()
	_ = r.store.Close()
}

func espGroupExists(dataRoot string) esphttp.GroupExistsFunc {
	return func(_ context.Context, groupID entmoot.GroupID) (bool, error) {
		gids, err := listGroupIDs(dataRoot, slog.Default())
		if err != nil {
			return false, err
		}
		for _, gid := range gids {
			if gid == groupID {
				return true, nil
			}
		}
		return false, nil
	}
}

func addrIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	switch strings.ToLower(host) {
	case "localhost":
		return true
	case "":
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
