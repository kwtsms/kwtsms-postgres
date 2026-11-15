// FILE:      internal/worker/batch.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Pure helpers: row grouping into batches, country allowlist, retry backoff, send-mode rule.
// WHY:       kwtSMS takes up to 200 numbers per request sharing one message and sender; keeping this logic
//            free of I/O makes it unit-testable without a database.
// USAGE:     batches := GroupBatches(rows, cfg)

package worker

import (
	"sort"
	"strings"
	"time"
)

// MaxBatch is the most numbers kwtSMS accepts in one send request (ERR007 above this).
const MaxBatch = 200

// Row is one claimed outbox row.
type Row struct {
	ID       int64
	Mobile   string
	Message  string
	Sender   string // empty = use the configured default
	Test     bool
	Attempts int
}

// Batch is a group of rows sent in one request.
type Batch struct {
	Message string
	Sender  string
	Test    bool // value for the API "test" field
	Rows    []Row
}

// EffectiveTest is true unless the worker is live AND the row asks for a real send.
func EffectiveTest(live bool, rowTest bool) bool {
	return !(live && !rowTest)
}

// GroupBatches groups rows by message, sender and effective test flag, at most MaxBatch rows each.
func GroupBatches(rows []Row, defaultSender string, live bool) []Batch {
	sorted := append([]Row(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	type key struct {
		msg, sender string
		test        bool
	}
	idx := map[key]int{} // key -> index of the batch currently being filled
	var out []Batch
	for _, r := range sorted {
		s := r.Sender
		if s == "" {
			s = defaultSender
		}
		k := key{r.Message, s, EffectiveTest(live, r.Test)}
		i, ok := idx[k]
		if !ok || len(out[i].Rows) >= MaxBatch {
			out = append(out, Batch{Message: k.msg, Sender: k.sender, Test: k.test})
			i = len(out) - 1
			idx[k] = i
		}
		out[i].Rows = append(out[i].Rows, r)
	}
	return out
}

// Allowed reports whether mobile starts with one of the allowed country prefixes.
func Allowed(mobile string, countries []string) bool {
	for _, p := range countries {
		if p != "" && strings.HasPrefix(mobile, p) {
			return true
		}
	}
	return false
}

// Backoff is the wait before retry number attempt (1 = first retry): 60 s doubling, capped at 15 min.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 60 * time.Second
	for i := 1; i < attempt && d < 15*time.Minute; i++ {
		d *= 2
	}
	if d > 15*time.Minute {
		d = 15 * time.Minute
	}
	return d
}

// Retryable reports whether an API error code may be retried: only ERR013 (more than 1000
// unscheduled messages waiting in the kwtSMS queue; the request was refused, nothing was sent).
func Retryable(code string) bool { return code == "ERR013" }
