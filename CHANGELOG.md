# Changelog

## [0.1.0] - 2026-11-16

- First release: send SMS from PostgreSQL through kwtSMS using an outbox table and a small worker.
- `kwtsms.enqueue()` and plain INSERT, number normalization and caps enforced inside the database.
- Worker: single static binary and a container image, safe to run several at once, test mode by default.
