// FILE:      internal/worker/fakeapi_test.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Test helper: a fake kwtSMS API (httptest) and a transport that redirects the library to it.
// WHY:       Tests never touch the network and never need credentials; the fake also records what was posted.
// USAGE:     f := newFakeAPI(t, nil); cfg.Transport = f.transport()

package worker

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

type fakeAPI struct {
	srv *httptest.Server
	mu  sync.Mutex
	// Reqs holds every decoded send payload.
	Reqs []map[string]any
	// UA holds the User-Agent of every request.
	UA []string
}

// handler may be nil: the default answers OK and reports one point per number.
func newFakeAPI(t *testing.T, handler func(w http.ResponseWriter, payload map[string]any)) *fakeAPI {
	f := &fakeAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p map[string]any
		_ = json.Unmarshal(body, &p)
		f.mu.Lock()
		f.Reqs = append(f.Reqs, p)
		f.UA = append(f.UA, r.UserAgent())
		n := len(f.Reqs)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if handler != nil {
			handler(w, p)
			return
		}
		cnt := len(strings.Split(p["mobile"].(string), ","))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": "OK", "msg-id": "msg" + string(rune('a'+n%26)), "numbers": cnt, "points-charged": cnt, "balance-after": 100,
		})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) transport() http.RoundTripper {
	u, _ := url.Parse(f.srv.URL)
	return redirect{u}
}

// requests returns a copy of the recorded payloads.
func (f *fakeAPI) requests() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.Reqs...)
}

type redirect struct{ to *url.URL }

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	c := req.Clone(req.Context())
	c.URL.Scheme, c.URL.Host = r.to.Scheme, r.to.Host
	return http.DefaultTransport.RoundTrip(c)
}

func writeErr(w http.ResponseWriter, status int, code, desc string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"result": "ERROR", "code": code, "description": desc})
}
