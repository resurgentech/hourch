// Command hourchd is the hourch job daemon (docs/DESIGN-hourchd.md):
//
//	hourchd serve        [--config /etc/hourchd/config.yaml]
//	hourchd check-config [--config /etc/hourchd/config.yaml]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/config"
)

const defaultConfig = "/etc/hourchd/config.yaml"

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  hourchd serve        [--config %[1]s]
  hourchd check-config [--config %[1]s]
`, defaultConfig)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(ctx, os.Args[2:])
	case "check-config":
		err = checkConfig(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "hourchd:", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func loadConfig(args []string, name string) (config.Config, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	path := fs.String("config", defaultConfig, "config file")
	_ = fs.Parse(args)
	return config.Load(*path)
}

func checkConfig(args []string) error {
	cfg, err := loadConfig(args, "check-config")
	if err != nil {
		return err
	}
	if err := app.Check(cfg); err != nil {
		return err
	}
	fmt.Println("config ok")
	return nil
}

func serve(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args, "serve")
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)
	a, err := app.Build(cfg, app.Deps{Log: log})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := a.Start(runCtx); err != nil {
		a.Store.Close()
		return err
	}
	srv := &http.Server{Addr: cfg.Listen, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("hourchd serving", "listen", cfg.Listen, "state_dir", cfg.StateDir, "bucket", cfg.MinIO.Bucket)
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	log.Info("hourchd shutting down")
	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	// SSE streams never go idle: end them first, or Shutdown waits out its
	// timeout. Hijacked websockets close in a.Close.
	a.API.CloseStreams()
	_ = srv.Shutdown(shCtx)
	cancel()
	if cerr := a.Close(); cerr != nil {
		log.Error("close", "error", cerr)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
