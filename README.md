# kwtsms-postgres

Send SMS from PostgreSQL through [kwtSMS](https://www.kwtsms.com). An outbox table, a SQL function, and a small worker.
Your app (or a trigger) queues a message with one statement; the worker sends it and writes the result back to the row.

All kwtSMS integrations: [kwtsms.com/integrations](https://www.kwtsms.com/integrations/)

## Quick start

```sh
psql "$DATABASE_URL" -f sql/install.sql                       # 1. schema "kwtsms" (safe to run twice)
psql "$DATABASE_URL" -c "SELECT kwtsms.enqueue('965XXXXXXXX', 'Your order is ready')"   # 2. queue one SMS
docker run --rm -e DATABASE_URL -e KWTSMS_USERNAME -e KWTSMS_PASSWORD \
  ghcr.io/kwtsms/kwtsms-pg-worker                             # 3. run the worker (test mode)
psql "$DATABASE_URL" -c "SELECT id, status, msg_id, error_code FROM kwtsms.outbox ORDER BY id DESC LIMIT 5"  # 4. result
```

The worker starts in **TEST MODE: nothing is delivered**. Messages are sent to kwtSMS with `test=1`: queued in your
kwtSMS account, never delivered, credits held until you delete them from the Sending Queue. To deliver for real you need
both switches: the worker with `KWTSMS_LIVE=1`, and the row queued with `test = false`:

```sql
SELECT kwtsms.enqueue('965XXXXXXXX', 'Your order is ready', 'MY-SENDER', false);   -- mobile, message, sender, test
```

Use your API username and password from the kwtSMS account (API information page), not your login. Replace
`965XXXXXXXX` with a real number in international format.

## Why an outbox

The HTTP call to the SMS gateway never runs inside your transaction. The SMS is queued in the same transaction as your
data (an order that rolls back sends nothing), and the worker does the slow network call outside.
Own table instead of pgmq: extensions are often missing on RDS, Neon or Supabase; this needs only plain SQL (PostgreSQL 12+).

## Install

| Piece | How |
|---|---|
| Schema | `psql "$DATABASE_URL" -f sql/install.sql` (idempotent), remove with `sql/uninstall.sql` |
| Worker binary | download from GitHub Releases (linux and macOS, amd64 and arm64), or `go install github.com/kwtsms/kwtsms-postgres/cmd/kwtsms-pg-worker@latest` |
| Worker image | `ghcr.io/kwtsms/kwtsms-pg-worker` (distroless, non-root) |

## Worker configuration (environment only)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | Postgres connection string |
| `KWTSMS_USERNAME`, `KWTSMS_PASSWORD` | required | kwtSMS API credentials |
| `KWTSMS_SENDER` | `KWT-SMS` | default Sender ID for rows without one (KWT-SMS is for testing only; register a private Sender ID for production) |
| `KWTSMS_LIVE` | `0` | `1` allows rows with `test = false` to be delivered |
| `KWTSMS_COUNTRIES` | `965` | allowed country prefixes, comma separated |
| `KWTSMS_MAX_PER_NUMBER_HOUR` | `5` | per-number cap per rolling hour (`0` = off) |
| `KWTSMS_MAX_DAILY` | `1000` | message cap per rolling 24 hours (`0` = off) |
| `KWTSMS_POLL_SECONDS` | `30` | fallback poll when no notification arrives |
| `KWTSMS_HTTP_TIMEOUT_SECONDS` | `30` | API request timeout |
| `KWTSMS_REAP_SECONDS` | `600` | a row stuck in `sending` longer than this becomes `unknown` |
| `KWTSMS_MAX_ATTEMPTS` | `5` | tries for ERR013 and network errors before a row fails |

## How a row moves

`queued` then `sending` then `sent`, `failed` or `unknown`.

- `sent`: kwtSMS accepted it. `msg_id` and `points` are stored (a batch shares one `msg_id`; points are split per row).
- `failed`: kwtSMS refused it. The code is in `error_code`, for example `ERR010` (zero balance) or `ERR028` (duplicate protection).
- `unknown`: the request was written but no answer came back (timeout), or the worker died mid-send. The row is **never resent**
  automatically, because it may already have been delivered and charged. Check the kwtSMS dashboard, then `SELECT kwtsms.requeue(id)`.
- Only `ERR013` (more than 1000 unscheduled messages waiting in your kwtSMS queue) and network errors that happen before
  the request leaves the host are retried, with a growing delay.

Several workers can run at once: rows are claimed with `FOR UPDATE SKIP LOCKED`, so no row is sent twice.

## Rules enforced in the database

A `BEFORE INSERT` trigger applies to `kwtsms.enqueue()` and to a direct `INSERT` alike:

- Normalization: Arabic and Persian digits become 0-9; `+`, `00`, spaces and dashes are removed. `+965 5000 0001`, `00965 50000001` and Arabic-Indic digits all store `965XXXXXXXX`.
- `CHECK`: the number must be 7 to 15 digits; the message must not be blank.
- Allowlist: `kwtsms.settings` key `allowed_countries` (default `965`).
- Caps: `max_per_number_hour` (default 5) and `max_daily_messages` (default 1000). The worker repeats the same checks.

```sql
UPDATE kwtsms.settings SET value = '965,966' WHERE key = 'allowed_countries';
```

## Operations

```sql
SELECT * FROM kwtsms.outbox_stats;          -- counts by status
SELECT kwtsms.requeue(42);                  -- retry a failed or unknown row (operator decision)
SELECT kwtsms.purge('90 days');             -- delete finished rows older than 90 days (keep 2 days or more for the caps)
```

## Least-privilege role

```sh
psql "$DATABASE_URL" -f sql/worker_role.sql     # role kwtsms_worker: SELECT, UPDATE on kwtsms.outbox, EXECUTE on kwtsms.reap
psql "$DATABASE_URL" -c "ALTER ROLE kwtsms_worker PASSWORD '...'"
```

Give your application only `GRANT USAGE ON SCHEMA kwtsms` and `GRANT EXECUTE ON FUNCTION kwtsms.enqueue(text, text, text, boolean) TO app_role`.

## Managed Postgres

The worker wakes on `LISTEN/NOTIFY` and polls every 30 seconds as a fallback. `LISTEN` needs a direct connection, not a
transaction pooler (PgBouncer, Supavisor); behind a pooler the worker still works by polling.

## Example

[examples/orders_trigger.sql](examples/orders_trigger.sql): an `orders` table whose trigger calls `kwtsms.enqueue()`.

## Develop

```sh
go vet ./... && go test ./...                                   # unit tests, fake API, no network
KWTSMS_TEST_DATABASE_URL=postgres://.../kwtsms_test go test -tags integration ./...   # needs Postgres; CI runs 12, 16, 17
```

The database name must contain `test`: the schema is dropped and rebuilt for every test.

## العربية

أرسل رسائل SMS من PostgreSQL عبر kwtSMS. تضيف الرسالة إلى جدول `kwtsms.outbox` بأمر SQL واحد، ويرسلها برنامج صغير يعمل في الخلفية ثم يكتب النتيجة في نفس الصف.

1. شغّل `sql/install.sql` على قاعدة البيانات.
2. أضف رسالة: `SELECT kwtsms.enqueue('965XXXXXXXX', 'طلبك جاهز')`.
3. شغّل `kwtsms-pg-worker` مع `DATABASE_URL` و`KWTSMS_USERNAME` و`KWTSMS_PASSWORD`.

يبدأ البرنامج في وضع التجربة: لا يتم تسليم أي رسالة. للتسليم الفعلي اضبط `KWTSMS_LIVE=1` وأضف الرسالة مع `test = false`.
استخدم اسم المرسل المسجل في حسابك على kwtSMS؛ اسم المرسل KWT-SMS للتجربة فقط. الرسائل التي لا يُعرف مصيرها (انتهاء المهلة) تُحفظ بحالة `unknown` ولا يُعاد إرسالها تلقائيا، حتى لا تصل مرتين.

## License

MIT. See [LICENSE](LICENSE). Security: see [SECURITY.md](SECURITY.md).
