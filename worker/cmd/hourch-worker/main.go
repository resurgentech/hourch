// Command hourch-worker runs hourchd jobs on this host.
//
//	hourch-worker --config /etc/hourch/worker.yaml
//
// Signals: the first SIGTERM or SIGINT drains — no new jobs are taken, the
// running job keeps running and its result is delivered, then the worker
// exits (at once when idle). A second SIGTERM/SIGINT stops now: the job's
// process group (or container) is killed and no result is sent, so its
// lease lapses and hourchd requeues or fails it. Under systemd, use
// KillMode=mixed so the stop signal reaches only the worker, not the job.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	hourchworker "github.com/resurgentech/hourch/worker"
)

func main() {
	cfgPath := flag.String("config", "/etc/hourch/worker.yaml", "worker config file")
	version := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *version {
		fmt.Println(hourchworker.Version)
		return
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*cfgPath, log); err != nil {
		log.Error("hourch-worker", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath string, log *slog.Logger) error {
	cfg, err := hourchworker.LoadConfig(cfgPath)
	if err != nil {
		return err
	}
	token, err := hourchworker.ReadTokenFile(cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("token_file: %w", err)
	}
	w, err := hourchworker.New(cfg, token, hourchworker.Options{Logger: log})
	if err != nil {
		return err
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigs
		log.Info("draining: no new jobs; the running job continues", "signal", s.String(), "job", w.Busy())
		w.Drain()
		s = <-sigs
		log.Warn("stopping now: killing the running job", "signal", s.String(), "job", w.Busy())
		stop()
	}()

	log.Info("hourch-worker starting", "version", hourchworker.Version, "worker_id", cfg.WorkerID, "hourchd_url", cfg.HourchdURL, "tags", cfg.Tags, "capabilities", cfg.Capabilities)
	return w.Run(ctx)
}
