// FILE:      internal/worker/store.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      All SQL the worker runs: claim, finish, reap, cap checks.
// WHY:       One place for the statements, so the privilege list in the README (SELECT, UPDATE, reap) stays true.
// USAGE:     used by Worker
// DEPENDS:   github.com/jackc/pgx/v5

package worker

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the connection pool.
type Store struct {
	Pool     *pgxpool.Pool
	WorkerID string
}

const claimSQL = `
UPDATE kwtsms.outbox o
   SET status = 'sending', locked_at = now(), locked_by = $1, attempts = o.attempts + 1
 WHERE o.id IN (SELECT id FROM kwtsms.outbox
                 WHERE status = 'queued' AND next_attempt_at <= now()
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2)
RETURNING o.id, o.mobile, o.message, coalesce(o.sender, ''), o.test, o.attempts`

// Claim takes up to limit queued rows; several workers can run at once, each row goes to exactly one.
func (s *Store) Claim(ctx context.Context, limit int) ([]Row, error) {
	rows, err := s.Pool.Query(ctx, claimSQL, s.WorkerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Mobile, &r.Message, &r.Sender, &r.Test, &r.Attempts); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID }) // RETURNING order is not defined
	return out, rows.Err()
}

const finishSQL = `
UPDATE kwtsms.outbox SET
       status = $2::text, msg_id = $3::text, points = $4::float8::numeric,
       error_code = $5::text, error_text = $6::text,
       sent_at = CASE WHEN $2::text = 'sent' THEN now() ELSE sent_at END,
       next_attempt_at = CASE WHEN $2::text = 'queued' THEN now() + make_interval(secs => $7::float8) ELSE next_attempt_at END,
       locked_at = CASE WHEN $2::text = 'queued' THEN NULL ELSE locked_at END,
       locked_by = CASE WHEN $2::text = 'queued' THEN NULL ELSE locked_by END
 WHERE id = ANY($1::bigint[]) AND locked_by = $8::text AND status IN ('sending', 'unknown')`

// Result is the state to write for a set of rows.
type Result struct {
	Status string // sent, failed, unknown, queued
	MsgID  string
	Points *float64
	Code   string
	Text   string
	Retry  time.Duration // queued only: wait before the next attempt
}

// Finish writes a result for ids. The context is detached from shutdown so a result is never lost.
func (s *Store) Finish(ctx context.Context, ids []int64, r Result) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var msgID, code, text *string
	if r.MsgID != "" {
		msgID = &r.MsgID
	}
	if r.Code != "" {
		code = &r.Code
	}
	if r.Text != "" {
		text = &r.Text
	}
	_, err := s.Pool.Exec(ctx, finishSQL, ids, r.Status, msgID, r.Points, code, text, r.Retry.Seconds(), s.WorkerID)
	return err
}

// Reap marks rows stuck in sending for longer than after as unknown (never resent).
func (s *Store) Reap(ctx context.Context, after time.Duration) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT kwtsms.reap(make_interval(secs => $1::float8))`, after.Seconds()).Scan(&n)
	return n, err
}

// HourExcess returns ids among ids that exceed max messages per number in the last hour
// (counting rows that are sending, sent or unknown, lowest id first).
func (s *Store) HourExcess(ctx context.Context, mobiles []string, ids []int64, max int) ([]int64, error) {
	rows, err := s.Pool.Query(ctx, `
SELECT id FROM (
  SELECT id, row_number() OVER (PARTITION BY mobile ORDER BY id) AS rn
    FROM kwtsms.outbox
   WHERE mobile = ANY($1::text[]) AND status IN ('sending', 'sent', 'unknown')
     AND locked_at > now() - interval '1 hour') t
 WHERE id = ANY($2::bigint[]) AND rn > $3`, mobiles, ids, max)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// DailyCount counts rows sending, sent or unknown in the last 24 hours.
func (s *Store) DailyCount(ctx context.Context) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM kwtsms.outbox
 WHERE status IN ('sending', 'sent', 'unknown') AND locked_at > now() - interval '24 hours'`).Scan(&n)
	return n, err
}
