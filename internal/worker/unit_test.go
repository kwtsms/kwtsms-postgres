// FILE:      internal/worker/unit_test.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Unit tests: config, batching, allowlist, backoff, and the API classification against a fake kwtSMS.
// WHY:       Proves the send fields, the test-mode rule, ERR013 handling, 4XX JSON bodies and the retry-vs-unknown
//            split without network or credentials.
// USAGE:     go test ./...

package worker

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	n1 = "96550000001"
	n2 = "96550000002"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestConfigDefaults(t *testing.T) {
	c, err := ConfigFromEnv(env(map[string]string{"DATABASE_URL": "postgres://x", "KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Live || c.Sender != "KWT-SMS" || len(c.Countries) != 1 || c.Countries[0] != "965" || c.PollInterval != 30*time.Second || c.ReapAfter != 10*time.Minute {
		t.Fatalf("bad defaults: %+v", c)
	}
	if !strings.HasPrefix(c.Banner(), "TEST MODE: nothing is delivered") {
		t.Fatalf("banner: %s", c.Banner())
	}
}

func TestConfigLiveOnlyWhenExactlyOne(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "x", "KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p"}
	for v, want := range map[string]bool{"1": true, "0": false, "true": false, "": false, "yes": false} {
		base["KWTSMS_LIVE"] = v
		c, _ := ConfigFromEnv(env(base))
		if c.Live != want {
			t.Errorf("KWTSMS_LIVE=%q live=%v want %v", v, c.Live, want)
		}
	}
	base["KWTSMS_LIVE"] = "1"
	c, _ := ConfigFromEnv(env(base))
	if !strings.HasPrefix(c.Banner(), "LIVE MODE") {
		t.Fatal(c.Banner())
	}
}

func TestConfigRequiredAndBadValues(t *testing.T) {
	for _, m := range []map[string]string{
		{"KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p"},
		{"DATABASE_URL": "x", "KWTSMS_PASSWORD": "p"},
		{"DATABASE_URL": "x", "KWTSMS_USERNAME": "u"},
		{"DATABASE_URL": "x", "KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p", "KWTSMS_MAX_DAILY": "abc"},
		{"DATABASE_URL": "x", "KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p", "KWTSMS_POLL_SECONDS": "0"},
	} {
		if _, err := ConfigFromEnv(env(m)); err == nil {
			t.Errorf("expected error for %v", m)
		}
	}
}

func TestConfigCountriesList(t *testing.T) {
	c, err := ConfigFromEnv(env(map[string]string{"DATABASE_URL": "x", "KWTSMS_USERNAME": "u", "KWTSMS_PASSWORD": "p", "KWTSMS_COUNTRIES": "965, 966 ,"}))
	if err != nil || len(c.Countries) != 2 || c.Countries[1] != "966" {
		t.Fatalf("%v %+v", err, c.Countries)
	}
}

func TestEffectiveTest(t *testing.T) {
	cases := []struct{ live, rowTest, want bool }{
		{false, true, true}, {false, false, true}, {true, true, true}, {true, false, false},
	}
	for _, c := range cases {
		if got := EffectiveTest(c.live, c.rowTest); got != c.want {
			t.Errorf("live=%v rowTest=%v got %v", c.live, c.rowTest, got)
		}
	}
}

func TestGroupBatchesSplitsBySenderMessageAndMode(t *testing.T) {
	rows := []Row{
		{ID: 3, Mobile: n1, Message: "a", Test: true},
		{ID: 1, Mobile: n2, Message: "a", Test: true},
		{ID: 2, Mobile: n1, Message: "b", Test: true},
		{ID: 4, Mobile: n1, Message: "a", Sender: "Shop", Test: true},
		{ID: 5, Mobile: n1, Message: "a", Test: false},
	}
	b := GroupBatches(rows, "KWT-SMS", true)
	if len(b) != 4 {
		t.Fatalf("want 4 batches got %d: %+v", len(b), b)
	}
	if b[0].Rows[0].ID != 1 || b[0].Rows[1].ID != 3 || b[0].Sender != "KWT-SMS" || !b[0].Test {
		t.Fatalf("first batch %+v", b[0])
	}
	last := b[len(b)-1]
	if last.Test || last.Rows[0].ID != 5 {
		t.Fatalf("live row must be its own non-test batch: %+v", last)
	}
	// worker not live: the test=false row joins the test batch
	if got := GroupBatches(rows, "KWT-SMS", false); len(got) != 3 {
		t.Fatalf("not live: want 3 batches got %d", len(got))
	}
}

func TestGroupBatchesLimit200(t *testing.T) {
	var rows []Row
	for i := 1; i <= 450; i++ {
		rows = append(rows, Row{ID: int64(i), Mobile: n1, Message: "m", Test: true})
	}
	b := GroupBatches(rows, "S", false)
	if len(b) != 3 || len(b[0].Rows) != 200 || len(b[1].Rows) != 200 || len(b[2].Rows) != 50 {
		t.Fatalf("got %d batches", len(b))
	}
}

func TestAllowedBackoffRetryable(t *testing.T) {
	if !Allowed(n1, []string{"965"}) || Allowed("9665x", []string{"965"}) || Allowed(n1, nil) {
		t.Fatal("allowlist")
	}
	if Backoff(1) != time.Minute || Backoff(2) != 2*time.Minute || Backoff(3) != 4*time.Minute || Backoff(20) != 15*time.Minute || Backoff(0) != time.Minute {
		t.Fatal("backoff")
	}
	if !Retryable("ERR013") || Retryable("ERR003") || Retryable("NETWORK") || Retryable("ERR028") {
		t.Fatal("retryable")
	}
}

func apiFor(f *fakeAPI, timeout time.Duration) *API {
	return &API{Username: "user", Password: "pass", Timeout: timeout, Transport: f.transport()}
}

func TestSendOKFieldsAndTestFlag(t *testing.T) {
	f := newFakeAPI(t, nil)
	out := apiFor(f, 5*time.Second).Send([]string{n1, n2}, "hello", "MyShop", true)
	if out.Kind != KindOK || out.Numbers != 2 || out.Points != 2 || out.MsgID == "" {
		t.Fatalf("%+v", out)
	}
	r := f.requests()
	if len(r) != 1 {
		t.Fatalf("requests: %d", len(r))
	}
	p := r[0]
	if p["test"] != "1" || p["sender"] != "MyShop" || p["mobile"] != n1+","+n2 || p["message"] != "hello" || p["username"] != "user" || p["password"] != "pass" {
		t.Fatalf("payload %+v", p)
	}
	if !strings.Contains(f.UA[0], "kwtsms-postgres/"+Version) || !strings.Contains(f.UA[0], "kwtsms-go/") {
		t.Fatalf("user agent %q", f.UA[0])
	}
}

func TestSendLiveFlagIsZeroOnlyWhenAsked(t *testing.T) {
	f := newFakeAPI(t, nil)
	a := apiFor(f, 5*time.Second)
	a.Send([]string{n1}, "x", "S", false)
	a.Send([]string{n1}, "x", "S", true)
	r := f.requests()
	if r[0]["test"] != "0" || r[1]["test"] != "1" {
		t.Fatalf("test fields: %v %v", r[0]["test"], r[1]["test"])
	}
}

func TestSendERR013IsAPIErrorFromHTTP406Body(t *testing.T) {
	f := newFakeAPI(t, func(w http.ResponseWriter, _ map[string]any) {
		writeErr(w, 406, "ERR013", "More than 1000 unscheduled messages are waiting in your queue.")
	})
	out := apiFor(f, 5*time.Second).Send([]string{n1}, "x", "S", true)
	if out.Kind != KindAPIError || out.Code != "ERR013" || !Retryable(out.Code) {
		t.Fatalf("%+v", out)
	}
}

func TestSend403BodyIsReadNotRetried(t *testing.T) {
	f := newFakeAPI(t, func(w http.ResponseWriter, _ map[string]any) {
		writeErr(w, 403, "ERR003", "Wrong username or password.")
	})
	out := apiFor(f, 5*time.Second).Send([]string{n1}, "x", "S", true)
	if out.Kind != KindAPIError || out.Code != "ERR003" || Retryable(out.Code) {
		t.Fatalf("%+v", out)
	}
}

func TestSendTimeoutAfterWriteIsAmbiguous(t *testing.T) {
	f := newFakeAPI(t, func(w http.ResponseWriter, _ map[string]any) { time.Sleep(400 * time.Millisecond) })
	out := apiFor(f, 100*time.Millisecond).Send([]string{n1}, "x", "S", true)
	if out.Kind != KindAmbiguous {
		t.Fatalf("%+v", out)
	}
}

func TestSendGarbageAnswerIsAmbiguous(t *testing.T) {
	f := newFakeAPI(t, func(w http.ResponseWriter, _ map[string]any) {
		w.WriteHeader(502)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
	})
	if out := apiFor(f, 5*time.Second).Send([]string{n1}, "x", "S", true); out.Kind != KindAmbiguous {
		t.Fatalf("%+v", out)
	}
}

func TestSendConnectionRefusedIsPreSend(t *testing.T) {
	f := newFakeAPI(t, nil)
	a := apiFor(f, 5*time.Second)
	f.srv.Close()
	out := a.Send([]string{n1}, "x", "S", true)
	if out.Kind != KindPreSend || out.Code != "NETWORK" {
		t.Fatalf("%+v", out)
	}
}

func TestSendLocallyInvalidNumberReported(t *testing.T) {
	f := newFakeAPI(t, nil)
	out := apiFor(f, 5*time.Second).Send([]string{n1, "96512"}, "x", "S", true)
	if out.Kind != KindOK || out.Invalid["96512"] == "" || out.Numbers != 1 {
		t.Fatalf("%+v", out)
	}
	if got := f.requests()[0]["mobile"]; got != n1 {
		t.Fatalf("only the valid number is posted, got %v", got)
	}
}
