// FILE:      internal/worker/worker.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      The worker loop: LISTEN or poll, claim, check, send, write results, reap.
// WHY:       Moves SMS sending out of the database transaction. Never resends a row whose outcome is unknown.
// USAGE:     w, _ := worker.Open(ctx, cfg); w.Run(ctx)   or   w.Cycle(ctx) in tests
// DEPENDS:   github.com/jackc/pgx/v5, github.com/kwtsms/kwtsms-go

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Worker sends queued outbox rows.
type Worker struct {
	cfg   Config
	store *Store
	api   *API
	log   *slog.Logger
}

// Open connects to the database.
func Open(ctx context.Context, cfg Config, log *slog.Logger) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.WorkerID == "" {
		h, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d", h, os.Getpid())
	}
	if log == nil {
		log = slog.Default()
	}
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Worker{
		cfg:   cfg,
		store: &Store{Pool: pool, WorkerID: cfg.WorkerID},
		api:   &API{Username: cfg.Username, Password: cfg.Password, Timeout: cfg.HTTPTimeout, Transport: cfg.Transport},
		log:   log,
	}, nil
}

// Close releases the pool.
func (w *Worker) Close() { w.store.Pool.Close() }

// Run loops until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("kwtsms-pg-worker starting", "version", Version, "worker", w.cfg.WorkerID, "mode", w.cfg.Banner())
	wake := make(chan struct{}, 1)
	go w.listen(ctx, wake)
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		if n, err := w.Cycle(ctx); err != nil && ctx.Err() == nil {
			w.log.Error("cycle failed", "err", err)
		} else if n > 0 {
			w.log.Info("cycle done", "rows", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-t.C:
		}
	}
}

// listen forwards NOTIFY wakeups. It needs a direct connection; behind a pooler it fails and polling still works.
func (w *Worker) listen(ctx context.Context, wake chan<- struct{}) {
	for ctx.Err() == nil {
		conn, err := pgx.Connect(ctx, w.cfg.DatabaseURL)
		if err == nil {
			if _, err = conn.Exec(ctx, "LISTEN kwtsms_outbox"); err == nil {
				for {
					if _, err = conn.WaitForNotification(ctx); err != nil {
						break
					}
					select {
					case wake <- struct{}{}:
					default:
					}
				}
			}
			_ = conn.Close(context.WithoutCancel(ctx))
		}
		if ctx.Err() != nil {
			return
		}
		w.log.Warn("LISTEN unavailable, polling only until it reconnects", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

// Cycle reaps stuck rows, then claims and processes until the queue is empty. It returns rows handled.
func (w *Worker) Cycle(ctx context.Context) (int, error) {
	if n, err := w.store.Reap(ctx, w.cfg.ReapAfter); err != nil {
		return 0, err
	} else if n > 0 {
		w.log.Warn("rows stuck in sending marked unknown (never resent)", "rows", n)
	}
	total := 0
	for ctx.Err() == nil {
		rows, err := w.store.Claim(ctx, w.cfg.BatchLimit)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			return total, nil
		}
		total += len(rows)
		if err := w.process(ctx, rows); err != nil {
			return total, err
		}
	}
	return total, nil
}

func ids(rows []Row) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func (w *Worker) fail(ctx context.Context, rows []Row, code, text string) error {
	return w.store.Finish(ctx, ids(rows), Result{Status: "failed", Code: code, Text: text})
}

func (w *Worker) process(ctx context.Context, rows []Row) error {
	rows, err := w.precheck(ctx, rows)
	if err != nil {
		return err
	}
	for _, b := range GroupBatches(rows, w.cfg.Sender, w.cfg.Live) {
		if err := w.sendBatch(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// precheck applies the same rules as the database trigger (allowlist, caps) and fails rows that break them.
func (w *Worker) precheck(ctx context.Context, rows []Row) ([]Row, error) {
	var ok, blocked []Row
	for _, r := range rows {
		if Allowed(r.Mobile, w.cfg.Countries) {
			ok = append(ok, r)
		} else {
			blocked = append(blocked, r)
		}
	}
	if err := w.fail(ctx, blocked, "COUNTRY_NOT_ALLOWED", "number prefix is not in KWTSMS_COUNTRIES"); err != nil {
		return nil, err
	}
	if w.cfg.MaxPerNumberHour > 0 && len(ok) > 0 {
		mobiles := make([]string, len(ok))
		for i, r := range ok {
			mobiles[i] = r.Mobile
		}
		ex, err := w.store.HourExcess(ctx, mobiles, ids(ok), w.cfg.MaxPerNumberHour)
		if err != nil {
			return nil, err
		}
		ok, err = w.split(ctx, ok, ex, "CAP_HOUR", "per-number hourly cap reached")
		if err != nil {
			return nil, err
		}
	}
	if w.cfg.MaxDaily > 0 && len(ok) > 0 {
		n, err := w.store.DailyCount(ctx)
		if err != nil {
			return nil, err
		}
		if over := n - w.cfg.MaxDaily; over > 0 {
			if over > len(ok) {
				over = len(ok)
			}
			ex := ids(ok[len(ok)-over:])
			if ok, err = w.split(ctx, ok, ex, "CAP_DAILY", "daily message cap reached"); err != nil {
				return nil, err
			}
		}
	}
	return ok, nil
}

// split fails the rows whose id is in excess and returns the others.
func (w *Worker) split(ctx context.Context, rows []Row, excess []int64, code, text string) ([]Row, error) {
	if len(excess) == 0 {
		return rows, nil
	}
	bad := map[int64]bool{}
	for _, id := range excess {
		bad[id] = true
	}
	var keep, drop []Row
	for _, r := range rows {
		if bad[r.ID] {
			drop = append(drop, r)
		} else {
			keep = append(keep, r)
		}
	}
	return keep, w.fail(ctx, drop, code, text)
}

func (w *Worker) sendBatch(ctx context.Context, b Batch) error {
	nums := make([]string, len(b.Rows))
	for i, r := range b.Rows {
		nums[i] = r.Mobile
	}
	out := w.api.Send(nums, b.Message, b.Sender, b.Test)

	// rows the client library rejected locally (bad number shape) fail on their own
	var good []Row
	for _, r := range b.Rows {
		if reason, bad := out.Invalid[r.Mobile]; bad {
			if err := w.fail(ctx, []Row{r}, "ERR_INVALID_INPUT", reason); err != nil {
				return err
			}
		} else {
			good = append(good, r)
		}
	}
	if len(good) == 0 {
		return nil
	}

	switch out.Kind {
	case KindOK:
		n := out.Numbers
		if n <= 0 {
			n = len(good)
		}
		p := float64(out.Points) / float64(n)
		return w.store.Finish(ctx, ids(good), Result{Status: "sent", MsgID: out.MsgID, Points: &p})
	case KindAmbiguous:
		w.log.Warn("request written but no usable answer; rows marked unknown, not resent", "rows", len(good), "err", out.Desc)
		return w.store.Finish(ctx, ids(good), Result{Status: "unknown", Code: "AMBIGUOUS", Text: "request was sent, answer lost: " + out.Desc})
	case KindPreSend:
		return w.retryOrFail(ctx, good, out)
	default: // KindAPIError
		if Retryable(out.Code) {
			return w.retryOrFail(ctx, good, out)
		}
		return w.fail(ctx, good, out.Code, out.Desc)
	}
}

// retryOrFail requeues rows with backoff until MaxAttempts, then fails them.
func (w *Worker) retryOrFail(ctx context.Context, rows []Row, out Outcome) error {
	var again, give []Row
	wait := time.Duration(0)
	for _, r := range rows {
		if r.Attempts >= w.cfg.MaxAttempts {
			give = append(give, r)
			continue
		}
		again = append(again, r)
		if d := Backoff(r.Attempts); d > wait {
			wait = d
		}
	}
	if err := w.store.Finish(ctx, ids(again), Result{Status: "queued", Code: out.Code, Text: out.Desc, Retry: wait}); err != nil {
		return err
	}
	return w.fail(ctx, give, out.Code, out.Desc)
}
