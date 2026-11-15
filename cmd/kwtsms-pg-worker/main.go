// FILE:      cmd/kwtsms-pg-worker/main.go
// CREATED:   2026-11-16 02:15 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Entry point of the kwtsms-pg-worker binary.
// WHY:       A single static binary that sends the rows queued in kwtsms.outbox.
// USAGE:     DATABASE_URL=... KWTSMS_USERNAME=... KWTSMS_PASSWORD=... kwtsms-pg-worker
// DEPENDS:   internal/worker

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kwtsms/kwtsms-postgres/internal/worker"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println("kwtsms-pg-worker", worker.Version)
		return
	}
	if len(os.Args) > 1 {
		fmt.Println("usage: kwtsms-pg-worker   (configured by environment: DATABASE_URL, KWTSMS_USERNAME, KWTSMS_PASSWORD, KWTSMS_SENDER, KWTSMS_LIVE, KWTSMS_COUNTRIES, KWTSMS_MAX_PER_NUMBER_HOUR, KWTSMS_MAX_DAILY, KWTSMS_POLL_SECONDS, KWTSMS_HTTP_TIMEOUT_SECONDS, KWTSMS_REAP_SECONDS, KWTSMS_MAX_ATTEMPTS)")
		return
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := worker.ConfigFromEnv(os.Getenv)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	w, err := worker.Open(ctx, cfg, log)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer w.Close()
	if err := w.Run(ctx); err != nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
}
