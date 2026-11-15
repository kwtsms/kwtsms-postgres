//go:build integration

// FILE:      internal/worker/integration_test.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      SQL and worker integration tests against a real PostgreSQL (GitHub Actions service container).
// WHY:       Proves normalization, CHECK, allowlist, caps, SKIP LOCKED claiming, reaper, ambiguous batches,
//            requeue, purge, stored ERR codes and the test-mode default on every supported Postgres version.
// USAGE:     KWTSMS_TEST_DATABASE_URL=postgres://...@host/kwtsms_test go test -tags integration ./...
//            The database name must contain "test"; the schema is dropped and rebuilt for every test.

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ctx = context.Background()

type env_ struct {
	url  string
	conn *pgx.Conn
}

func (e *env_) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func readSQL(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "sql", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// setup connects, proves the target is a test database, and rebuilds the schema from sql/install.sql.
func setup(t *testing.T) *env_ {
	t.Helper()
	u := os.Getenv("KWTSMS_TEST_DATABASE_URL")
	if u == "" {
		t.Skip("KWTSMS_TEST_DATABASE_URL not set")
	}
	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg.Database, "test") {
		t.Fatalf("refusing to run: database %q does not contain \"test\"", cfg.Database)
	}
	conn, err := pgx.Connect(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	t.Logf("target database: %s on %s", cfg.Database, cfg.Host)
	e := &env_{url: u, conn: conn}
	e.exec(t, readSQL(t, "uninstall.sql"))
	e.exec(t, readSQL(t, "install.sql"))
	return e
}

func (e *env_) enq(t *testing.T, mobile, msg string, test bool) int64 {
	t.Helper()
	var id int64
	if err := e.conn.QueryRow(ctx, `SELECT kwtsms.enqueue($1, $2, NULL, $3)`, mobile, msg, test).Scan(&id); err != nil {
		t.Fatalf("enqueue %q: %v", mobile, err)
	}
	return id
}

type state struct {
	Status, MsgID, Code, By string
	Points                  float64
	Attempts                int
	Sent                    bool
}

func (e *env_) state(t *testing.T, id int64) state {
	t.Helper()
	var s state
	err := e.conn.QueryRow(ctx, `SELECT status, coalesce(msg_id,''), coalesce(error_code,''), coalesce(locked_by,''),
		coalesce(points,0)::float8, attempts, sent_at IS NOT NULL FROM kwtsms.outbox WHERE id = $1`, id).
		Scan(&s.Status, &s.MsgID, &s.Code, &s.By, &s.Points, &s.Attempts, &s.Sent)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *env_) sqlState(err error) string {
	if pe, ok := err.(*pgconn.PgError); ok {
		return pe.Code + " " + pe.ConstraintName
	}
	return ""
}

func baseCfg(url string, f *fakeAPI) Config {
	return Config{
		DatabaseURL: url, Username: "user", Password: "pass", Sender: "KWT-SMS", Countries: []string{"965"},
		MaxPerNumberHour: 5, MaxDaily: 1000, PollInterval: time.Second, HTTPTimeout: 5 * time.Second,
		ReapAfter: 10 * time.Minute, BatchLimit: 200, MaxAttempts: 5, WorkerID: "w1", Transport: f.transport(),
	}
}

func open(t *testing.T, cfg Config) *Worker {
	t.Helper()
	w, err := Open(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return w
}

func cycle(t *testing.T, w *Worker) int {
	t.Helper()
	n, err := w.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func okHandler(delay time.Duration) func(http.ResponseWriter, map[string]any) {
	return func(w http.ResponseWriter, p map[string]any) {
		time.Sleep(delay)
		n := len(strings.Split(p["mobile"].(string), ","))
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "OK", "msg-id": "m" + p["mobile"].(string)[:11], "numbers": n, "points-charged": n * 2, "balance-after": 50})
	}
}

func TestInstallIsIdempotentAndUninstallClean(t *testing.T) {
	e := setup(t)
	id := e.enq(t, "96550000001", "keep me", true)
	e.exec(t, readSQL(t, "install.sql")) // second run must not fail or lose data
	e.exec(t, readSQL(t, "install.sql"))
	if s := e.state(t, id); s.Status != "queued" {
		t.Fatalf("row lost after reinstall: %+v", s)
	}
	var n int
	_ = e.conn.QueryRow(ctx, `SELECT count(*) FROM kwtsms.settings`).Scan(&n)
	if n != 3 {
		t.Fatalf("settings rows: %d", n)
	}
	e.exec(t, readSQL(t, "uninstall.sql"))
	var exists bool
	_ = e.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kwtsms')`).Scan(&exists)
	if exists {
		t.Fatal("schema still there after uninstall")
	}
}

func TestNormalizationInSQL(t *testing.T) {
	e := setup(t)
	cases := map[string]string{
		"96550000001":     "96550000001",
		"+96550000001":    "96550000001",
		"0096550000001":   "96550000001",
		"+965 5000 0001":  "96550000001",
		"965-5000-0001":   "96550000001",
		"(965) 5000.0001": "96550000001",
		"00965 5000 0001": "96550000001",
		"٩٦٥٥٠٠٠٠٠٠١":     "96550000001", // Arabic-Indic digits
		"۹۶۵۵۰۰۰۰۰۰۱":     "96550000001", // Persian digits
		"+٩٦٥ 5000 0001":  "96550000001", // mixed
		"96505 0000001":   "96550000001", // trunk zero after the country code
	}
	for in, want := range cases {
		var got string
		if err := e.conn.QueryRow(ctx, `SELECT kwtsms.normalize_mobile($1)`, in).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
	// and end to end through enqueue()
	id := e.enq(t, "+٩٦٥ 5000 0002", "hi", true)
	var m string
	_ = e.conn.QueryRow(ctx, `SELECT mobile FROM kwtsms.outbox WHERE id=$1`, id).Scan(&m)
	if m != "96550000002" {
		t.Fatalf("stored %q", m)
	}
}

func TestCheckRefusesBadNumbersAndMessages(t *testing.T) {
	e := setup(t)
	for _, in := range []string{"abc", "", "12345", "9651234567890123456"} {
		_, err := e.conn.Exec(ctx, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ($1, 'x')`, in)
		if got := e.sqlState(err); got != "23514 outbox_mobile_chk" {
			t.Errorf("mobile %q: %q (%v)", in, got, err)
		}
	}
	_, err := e.conn.Exec(ctx, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ('96550000001', '   ')`)
	if got := e.sqlState(err); got != "23514 outbox_message_chk" {
		t.Errorf("blank message: %q", got)
	}
	_, err = e.conn.Exec(ctx, `INSERT INTO kwtsms.outbox (mobile, message, status) VALUES ('96550000001', 'x', 'bogus')`)
	if got := e.sqlState(err); got != "23514 outbox_status_chk" {
		t.Errorf("status: %q", got)
	}
}

func TestAllowlistRefusesOtherCountries(t *testing.T) {
	e := setup(t)
	_, err := e.conn.Exec(ctx, `SELECT kwtsms.enqueue('966500000001', 'x')`)
	if got := e.sqlState(err); !strings.HasPrefix(got, "KW001") {
		t.Fatalf("want KW001, got %q (%v)", got, err)
	}
	e.exec(t, `UPDATE kwtsms.settings SET value = '965, 966' WHERE key = 'allowed_countries'`)
	e.enq(t, "966500000001", "x", true)
}

func TestCapsTriggerAppliesToDirectInsert(t *testing.T) {
	e := setup(t)
	e.exec(t, `UPDATE kwtsms.settings SET value = '3' WHERE key = 'max_per_number_hour'`)
	for i := 0; i < 3; i++ {
		e.exec(t, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ('+965 5000 0001', 'x')`) // plain INSERT, no enqueue()
	}
	_, err := e.conn.Exec(ctx, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ('96550000001', 'x')`)
	if got := e.sqlState(err); !strings.HasPrefix(got, "KW002") {
		t.Fatalf("hourly cap: %q (%v)", got, err)
	}
	e.enq(t, "96550000002", "other number is fine", true)
	e.exec(t, `UPDATE kwtsms.settings SET value = '4' WHERE key = 'max_daily_messages'`)
	_, err = e.conn.Exec(ctx, `SELECT kwtsms.enqueue('96550000003', 'x')`)
	if got := e.sqlState(err); !strings.HasPrefix(got, "KW003") {
		t.Fatalf("daily cap: %q (%v)", got, err)
	}
}

func TestSkipLockedTwoWorkersNeverSendARowTwice(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, okHandler(15*time.Millisecond))
	const total = 80
	var ids []int64
	for i := 0; i < total; i++ {
		ids = append(ids, e.enq(t, fmt.Sprintf("965500%05d", i), "same text", true))
	}
	var ws []*Worker
	for _, name := range []string{"w1", "w2", "w3"} {
		c := baseCfg(e.url, f)
		c.WorkerID, c.BatchLimit = name, 7
		ws = append(ws, open(t, c))
	}
	var wg sync.WaitGroup
	for _, w := range ws {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			for {
				n, err := w.Cycle(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}(w)
	}
	wg.Wait()
	seen := map[string]int{}
	for _, r := range f.requests() {
		for _, m := range strings.Split(r["mobile"].(string), ",") {
			seen[m]++
		}
	}
	if len(seen) != total {
		t.Fatalf("distinct numbers posted: %d, want %d", len(seen), total)
	}
	for m, n := range seen {
		if n != 1 {
			t.Errorf("%s posted %d times", m, n)
		}
	}
	workers := map[string]bool{}
	for _, id := range ids {
		s := e.state(t, id)
		if s.Status != "sent" {
			t.Fatalf("row %d: %+v", id, s)
		}
		workers[s.By] = true
	}
	t.Logf("rows were claimed by %d different workers", len(workers))
}

func TestSentRowStoresMsgIDAndPointsPerRow(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, okHandler(0)) // 2 points per number
	a, b := e.enq(t, "96550000001", "hello", true), e.enq(t, "96550000002", "hello", true)
	w := open(t, baseCfg(e.url, f))
	if cycle(t, w) != 2 {
		t.Fatal("rows handled")
	}
	if len(f.requests()) != 1 {
		t.Fatalf("one batch expected, got %d requests", len(f.requests()))
	}
	for _, id := range []int64{a, b} {
		s := e.state(t, id)
		if s.Status != "sent" || s.MsgID != "m96550000001" || s.Points != 2 || !s.Sent || s.Attempts != 1 {
			t.Fatalf("%+v", s)
		}
	}
}

func TestReaperMarksStuckRowsUnknownAndNeverResends(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, nil)
	id := e.enq(t, "96550000001", "x", true)
	e.exec(t, `UPDATE kwtsms.outbox SET status='sending', locked_at = now() - interval '20 minutes', locked_by='dead' WHERE id=$1`, id)
	young := e.enq(t, "96550000002", "x", true)
	e.exec(t, `UPDATE kwtsms.outbox SET status='sending', locked_at = now() - interval '1 minute', locked_by='alive' WHERE id=$1`, young)
	w := open(t, baseCfg(e.url, f))
	cycle(t, w)
	if s := e.state(t, id); s.Status != "unknown" || s.Code != "REAPED" {
		t.Fatalf("%+v", s)
	}
	if s := e.state(t, young); s.Status != "sending" {
		t.Fatalf("young row touched: %+v", s)
	}
	cycle(t, w)
	if len(f.requests()) != 0 {
		t.Fatal("reaped row was sent")
	}
}

func TestAmbiguousTimeoutGoesUnknownNotResent(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, func(w http.ResponseWriter, p map[string]any) { time.Sleep(600 * time.Millisecond) })
	a, b := e.enq(t, "96550000001", "x", true), e.enq(t, "96550000002", "x", true)
	c := baseCfg(e.url, f)
	c.HTTPTimeout = 200 * time.Millisecond
	w := open(t, c)
	cycle(t, w)
	for _, id := range []int64{a, b} {
		if s := e.state(t, id); s.Status != "unknown" || s.Code != "AMBIGUOUS" {
			t.Fatalf("%+v", s)
		}
	}
	cycle(t, w)
	cycle(t, w)
	if n := len(f.requests()); n != 1 {
		t.Fatalf("batch was sent %d times", n)
	}
}

func TestPreSendFailureIsRetriedThenFails(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, nil)
	c := baseCfg(e.url, f)
	c.MaxAttempts = 2
	f.srv.Close() // connection refused: nothing was written
	id := e.enq(t, "96550000001", "x", true)
	w := open(t, c)
	cycle(t, w)
	if s := e.state(t, id); s.Status != "queued" || s.Code != "NETWORK" || s.Attempts != 1 {
		t.Fatalf("first: %+v", s)
	}
	e.exec(t, `UPDATE kwtsms.outbox SET next_attempt_at = now() WHERE id=$1`, id)
	cycle(t, w)
	if s := e.state(t, id); s.Status != "failed" || s.Code != "NETWORK" || s.Attempts != 2 {
		t.Fatalf("second: %+v", s)
	}
}

func TestERR013IsRetriedOthersFailWithCodeStored(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, func(w http.ResponseWriter, p map[string]any) {
		if strings.HasPrefix(p["message"].(string), "queuefull") {
			writeErr(w, 406, "ERR013", "More than 1000 unscheduled messages are waiting in your queue.")
			return
		}
		writeErr(w, 406, "ERR028", "Sent to the same number too soon.")
	})
	full := e.enq(t, "96550000001", "queuefull", true)
	dup := e.enq(t, "96550000002", "other", true)
	w := open(t, baseCfg(e.url, f))
	cycle(t, w)
	s := e.state(t, full)
	if s.Status != "queued" || s.Code != "ERR013" || s.Attempts != 1 {
		t.Fatalf("ERR013 row: %+v", s)
	}
	var wait float64
	_ = e.conn.QueryRow(ctx, `SELECT extract(epoch FROM next_attempt_at - now()) FROM kwtsms.outbox WHERE id=$1`, full).Scan(&wait)
	if wait < 30 || wait > 70 {
		t.Fatalf("backoff seconds: %v", wait)
	}
	if s := e.state(t, dup); s.Status != "failed" || s.Code != "ERR028" {
		t.Fatalf("ERR028 row: %+v", s)
	}
	cycle(t, w) // not due yet: must not be resent
	if n := len(f.requests()); n != 2 {
		t.Fatalf("requests: %d", n)
	}
}

func TestLocallyInvalidNumberFailsRowOthersSend(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, okHandler(0))
	good := e.enq(t, "96550000001", "x", true)
	bad := e.enq(t, "96512345", "x", true) // passes SQL, rejected by the client library (8 local digits needed)
	cycle(t, open(t, baseCfg(e.url, f)))
	if s := e.state(t, good); s.Status != "sent" {
		t.Fatalf("%+v", s)
	}
	if s := e.state(t, bad); s.Status != "failed" || s.Code != "ERR_INVALID_INPUT" {
		t.Fatalf("%+v", s)
	}
}

func TestRequeueAndPurge(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, func(w http.ResponseWriter, p map[string]any) { writeErr(w, 406, "ERR010", "Zero balance.") })
	id := e.enq(t, "96550000001", "x", true)
	w := open(t, baseCfg(e.url, f))
	cycle(t, w)
	if s := e.state(t, id); s.Status != "failed" || s.Code != "ERR010" {
		t.Fatalf("%+v", s)
	}
	var ok bool
	_ = e.conn.QueryRow(ctx, `SELECT kwtsms.requeue($1)`, id).Scan(&ok)
	if s := e.state(t, id); !ok || s.Status != "queued" || s.Code != "" || s.Attempts != 0 {
		t.Fatalf("requeue: %v %+v", ok, s)
	}
	_ = e.conn.QueryRow(ctx, `SELECT kwtsms.requeue($1)`, id).Scan(&ok)
	if ok {
		t.Fatal("a queued row must not be requeued")
	}

	// purge removes only old finished rows
	oldSent := e.enq(t, "96550000002", "x", true)
	oldQueued := e.enq(t, "96550000003", "x", true)
	fresh := e.enq(t, "96550000004", "x", true)
	e.exec(t, `UPDATE kwtsms.outbox SET status='sent', created_at = now() - interval '100 days' WHERE id=$1`, oldSent)
	e.exec(t, `UPDATE kwtsms.outbox SET created_at = now() - interval '100 days' WHERE id=$1`, oldQueued)
	var n int64
	_ = e.conn.QueryRow(ctx, `SELECT kwtsms.purge('90 days')`).Scan(&n)
	if n != 1 {
		t.Fatalf("purged %d", n)
	}
	var left int
	_ = e.conn.QueryRow(ctx, `SELECT count(*) FROM kwtsms.outbox WHERE id = ANY($1)`, []int64{oldQueued, fresh}).Scan(&left)
	if left != 2 {
		t.Fatalf("left %d", left)
	}
}

func TestTestModeIsDefaultAndLiveNeedsBothSwitches(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, okHandler(0))
	// default enqueue (test true), worker live: still test "1"
	var id int64
	_ = e.conn.QueryRow(ctx, `SELECT kwtsms.enqueue('96550000001', 'default row')`).Scan(&id)
	// worker not live, row asks for real: still test "1"
	e.enq(t, "96550000002", "wants real, worker off", false)
	cfg := baseCfg(e.url, f)
	cfg.Live = false
	cycle(t, open(t, cfg))
	cfg.Live = true
	// worker live and row real: "0"
	e.enq(t, "96550000003", "real row", false)
	cycle(t, open(t, cfg))
	cfg.Live = true
	e.enq(t, "96550000004", "default row live worker", true)
	cycle(t, open(t, cfg))

	got := map[string]string{}
	for _, r := range f.requests() {
		got[r["message"].(string)] = r["test"].(string)
	}
	want := map[string]string{"default row": "1", "wants real, worker off": "1", "real row": "0", "default row live worker": "1"}
	for m, v := range want {
		if got[m] != v {
			t.Errorf("message %q sent with test=%q, want %q", m, got[m], v)
		}
	}
	var def bool
	_ = e.conn.QueryRow(ctx, `SELECT test FROM kwtsms.outbox WHERE id=$1`, id).Scan(&def)
	if !def {
		t.Fatal("column default must be test = true")
	}
}

func TestWorkerSideChecksCatchRowsThatBypassedTheTrigger(t *testing.T) {
	e := setup(t)
	f := newFakeAPI(t, okHandler(0))
	e.exec(t, `ALTER TABLE kwtsms.outbox DISABLE TRIGGER outbox_before_insert`)
	var ids []int64
	for _, m := range []string{"96550000001", "96550000001", "96550000001", "96650000001"} {
		var id int64
		if err := e.conn.QueryRow(ctx, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ($1, 'x') RETURNING id`, m).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	c := baseCfg(e.url, f)
	c.MaxPerNumberHour = 2
	cycle(t, open(t, c))
	want := []string{"sent", "sent", "failed", "failed"}
	codes := []string{"", "", "CAP_HOUR", "COUNTRY_NOT_ALLOWED"}
	for i, id := range ids {
		if s := e.state(t, id); s.Status != want[i] || s.Code != codes[i] {
			t.Errorf("row %d: %+v", i, s)
		}
	}

	// daily cap in the worker
	e.exec(t, `TRUNCATE kwtsms.outbox`)
	for i := 0; i < 3; i++ {
		e.exec(t, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ('965500000' || $1::text, 'x')`, strconv.Itoa(10+i))
	}
	c.MaxDaily = 2
	cycle(t, open(t, c))
	var sent, capped int
	_ = e.conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='sent'), count(*) FILTER (WHERE error_code='CAP_DAILY') FROM kwtsms.outbox`).Scan(&sent, &capped)
	if sent != 2 || capped != 1 {
		t.Fatalf("sent %d capped %d", sent, capped)
	}
}

func TestLeastPrivilegeWorkerRole(t *testing.T) {
	e := setup(t)
	e.exec(t, readSQL(t, "worker_role.sql"))
	e.exec(t, `ALTER ROLE kwtsms_worker PASSWORD 'worker-test-pw'`)
	t.Cleanup(func() {
		e.exec(t, `DROP OWNED BY kwtsms_worker`)
		e.exec(t, `DROP ROLE IF EXISTS kwtsms_worker`)
	})
	u, err := url.Parse(e.url)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("kwtsms_worker", "worker-test-pw")
	f := newFakeAPI(t, okHandler(0))
	id := e.enq(t, "96550000001", "as the worker role", true)
	cycle(t, open(t, baseCfg(u.String(), f)))
	if s := e.state(t, id); s.Status != "sent" {
		t.Fatalf("%+v", s)
	}
	wc, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer wc.Close(ctx)
	for _, q := range []string{`DELETE FROM kwtsms.outbox`, `INSERT INTO kwtsms.outbox (mobile, message) VALUES ('96550000009','x')`, `SELECT kwtsms.enqueue('96550000009','x')`, `SELECT kwtsms.purge()`, `UPDATE kwtsms.settings SET value='1'`} {
		if _, err := wc.Exec(ctx, q); err == nil {
			t.Errorf("worker role must not be able to: %s", q)
		}
	}
}

func TestNotifyOnInsert(t *testing.T) {
	e := setup(t)
	l, err := pgx.Connect(ctx, e.url)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx)
	if _, err := l.Exec(ctx, `LISTEN kwtsms_outbox`); err != nil {
		t.Fatal(err)
	}
	id := e.enq(t, "96550000001", "x", true)
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := l.WaitForNotification(c)
	if err != nil {
		t.Fatal(err)
	}
	if n.Payload != strconv.FormatInt(id, 10) {
		t.Fatalf("payload %q", n.Payload)
	}
}

func TestOutboxStatsView(t *testing.T) {
	e := setup(t)
	e.enq(t, "96550000001", "x", true)
	e.enq(t, "96550000002", "x", true)
	var status string
	var n int
	if err := e.conn.QueryRow(ctx, `SELECT status, messages FROM kwtsms.outbox_stats`).Scan(&status, &n); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || n != 2 {
		t.Fatalf("%s %d", status, n)
	}
}
