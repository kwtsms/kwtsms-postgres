// FILE:      internal/worker/config.go
// CREATED:   2026-11-16 01:56 +03
// TASK:      int-postgres
// AUTHOR:    M Alhassoun
// WHAT:      Worker configuration read from environment variables only.
// WHY:       One static binary, no config file: secrets stay in the environment of the container or service.
// USAGE:     cfg, err := worker.ConfigFromEnv(os.Getenv)

package worker

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Version is sent to kwtSMS as the app token (kwtsms-postgres/<Version>).
const Version = "0.1.0"

// Config holds every worker setting.
type Config struct {
	DatabaseURL      string
	Username         string
	Password         string
	Sender           string        // default sender ID when a row has none
	Live             bool          // KWTSMS_LIVE=1; anything else keeps every send in test mode
	Countries        []string      // allowed country prefixes
	MaxPerNumberHour int           // 0 disables the worker-side check
	MaxDaily         int           // 0 disables the worker-side check
	PollInterval     time.Duration // fallback poll when no NOTIFY arrives
	HTTPTimeout      time.Duration
	ReapAfter        time.Duration
	BatchLimit       int // rows claimed per cycle
	MaxAttempts      int
	WorkerID         string
	Transport        http.RoundTripper // nil = default; tests point it at a fake API
}

// ConfigFromEnv builds a Config from getenv (os.Getenv in production).
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	c := Config{
		DatabaseURL:      getenv("DATABASE_URL"),
		Username:         getenv("KWTSMS_USERNAME"),
		Password:         getenv("KWTSMS_PASSWORD"),
		Sender:           getenv("KWTSMS_SENDER"),
		Live:             strings.TrimSpace(getenv("KWTSMS_LIVE")) == "1",
		MaxPerNumberHour: 5,
		MaxDaily:         1000,
		PollInterval:     30 * time.Second,
		HTTPTimeout:      30 * time.Second,
		ReapAfter:        10 * time.Minute,
		BatchLimit:       200,
		MaxAttempts:      5,
		WorkerID:         getenv("KWTSMS_WORKER_ID"),
	}
	if c.Sender == "" {
		c.Sender = "KWT-SMS"
	}
	countries := getenv("KWTSMS_COUNTRIES")
	if strings.TrimSpace(countries) == "" {
		countries = "965"
	}
	for _, p := range strings.Split(countries, ",") {
		if p = strings.TrimSpace(p); p != "" {
			c.Countries = append(c.Countries, p)
		}
	}
	var err error
	if c.MaxPerNumberHour, err = envInt(getenv, "KWTSMS_MAX_PER_NUMBER_HOUR", c.MaxPerNumberHour); err != nil {
		return c, err
	}
	if c.MaxDaily, err = envInt(getenv, "KWTSMS_MAX_DAILY", c.MaxDaily); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = envInt(getenv, "KWTSMS_MAX_ATTEMPTS", c.MaxAttempts); err != nil {
		return c, err
	}
	var secs int
	if secs, err = envInt(getenv, "KWTSMS_POLL_SECONDS", int(c.PollInterval/time.Second)); err != nil {
		return c, err
	}
	c.PollInterval = time.Duration(secs) * time.Second
	if secs, err = envInt(getenv, "KWTSMS_HTTP_TIMEOUT_SECONDS", int(c.HTTPTimeout/time.Second)); err != nil {
		return c, err
	}
	c.HTTPTimeout = time.Duration(secs) * time.Second
	if secs, err = envInt(getenv, "KWTSMS_REAP_SECONDS", int(c.ReapAfter/time.Second)); err != nil {
		return c, err
	}
	c.ReapAfter = time.Duration(secs) * time.Second
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Validate reports the first missing required setting.
func (c Config) Validate() error {
	switch {
	case c.DatabaseURL == "":
		return errors.New("DATABASE_URL is required")
	case c.Username == "":
		return errors.New("KWTSMS_USERNAME is required")
	case c.Password == "":
		return errors.New("KWTSMS_PASSWORD is required")
	case c.PollInterval <= 0 || c.HTTPTimeout <= 0 || c.ReapAfter <= 0:
		return errors.New("poll, http timeout and reap seconds must be positive")
	}
	return nil
}

// Banner is the startup line about the sending mode.
func (c Config) Banner() string {
	if c.Live {
		return "LIVE MODE: rows with test=false are delivered and charged; rows with test=true stay in test mode"
	}
	return "TEST MODE: nothing is delivered (set KWTSMS_LIVE=1 to deliver rows queued with test=false)"
}

func envInt(getenv func(string) string, key string, def int) (int, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", key, v)
	}
	return n, nil
}
