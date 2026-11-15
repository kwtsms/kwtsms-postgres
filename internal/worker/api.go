// FILE:      internal/worker/api.go
// CREATED:   2026-11-16 02:10 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Sends one batch through the kwtsms-go library and classifies the outcome.
// WHY:       The worker must know whether a failed request may be retried (nothing left this host) or must be
//            marked unknown (the request was written, the answer was lost). A resend could double-charge.
// USAGE:     out := (&API{...}).Send(numbers, message, sender, test)
// DEPENDS:   github.com/kwtsms/kwtsms-go

package worker

import (
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	kwtsms "github.com/kwtsms/kwtsms-go"
)

// Kind classifies a send attempt.
type Kind int

const (
	KindOK        Kind = iota // accepted by kwtSMS
	KindAPIError              // kwtSMS answered with an error (nothing sent, nothing charged)
	KindPreSend               // transport failed before the request was fully written: safe to retry
	KindAmbiguous             // request was written, no usable answer: outcome unknown, never resend
)

// Outcome is the classified result of one batch.
type Outcome struct {
	Kind    Kind
	MsgID   string
	Numbers int // numbers kwtSMS accepted
	Points  int // points-charged for the whole batch
	Code    string
	Desc    string
	Invalid map[string]string // number -> reason, rejected locally by the client library
}

// API holds the account settings for sending.
type API struct {
	Username  string
	Password  string
	Timeout   time.Duration
	Transport http.RoundTripper // nil = http.DefaultTransport
}

// tracingTransport records whether the request body was fully written to the connection.
type tracingTransport struct {
	base  http.RoundTripper
	wrote atomic.Bool
}

func (t *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	trace := &httptrace.ClientTrace{WroteRequest: func(i httptrace.WroteRequestInfo) {
		if i.Err == nil {
			t.wrote.Store(true)
		}
	}}
	return t.base.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
}

// Send posts one batch (at most 200 normalized numbers sharing message and sender).
// test=true sends "test":"1": queued at kwtSMS, never delivered.
func (a *API) Send(numbers []string, message, sender string, test bool) Outcome {
	base := a.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	tt := &tracingTransport{base: base}
	client, err := kwtsms.New(a.Username, a.Password,
		kwtsms.WithSenderID(sender),
		kwtsms.WithTestMode(test),
		kwtsms.WithLogFile(""),
		kwtsms.WithHTTPClient(&http.Client{Transport: tt, Timeout: a.Timeout}),
		kwtsms.WithAppName("kwtsms-postgres/"+Version),
	)
	if err != nil {
		return Outcome{Kind: KindAPIError, Code: "CONFIG", Desc: err.Error()}
	}
	res, err := client.SendMulti(numbers, message, sender)
	if err != nil || res == nil {
		d := "no result"
		if err != nil {
			d = err.Error()
		}
		return Outcome{Kind: KindAPIError, Code: "CLIENT", Desc: d}
	}
	invalid := map[string]string{}
	for _, in := range res.Invalid {
		invalid[in.Input] = in.Error
	}
	switch {
	case res.Result == "OK":
		return Outcome{Kind: KindOK, MsgID: res.MsgID, Numbers: res.Numbers, Points: res.PointsCharged, Invalid: invalid}
	case res.Code == "NETWORK":
		k := KindPreSend
		if tt.wrote.Load() {
			k = KindAmbiguous
		}
		return Outcome{Kind: k, Code: "NETWORK", Desc: res.Description, Invalid: invalid}
	default:
		return Outcome{Kind: KindAPIError, Code: res.Code, Desc: res.Description, Invalid: invalid}
	}
}
