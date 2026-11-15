-- FILE:      sql/install.sql
-- CREATED:   2026-11-16 01:56 +03
-- TASK:      int-postgres
-- AUTHOR:    M Alhassoun
-- WHAT:      Installs the kwtsms schema: outbox table, settings, normalization, enqueue(), caps trigger,
--            reaper, requeue(), purge(), outbox_stats view. Safe to run more than once.
-- WHY:       SMS is queued by plain SQL (a function or an INSERT); the worker sends it. The database itself
--            refuses bad numbers, blocked countries and over-cap inserts, so no client can bypass the rules.
-- USAGE:     psql "$DATABASE_URL" -f sql/install.sql      (needs PostgreSQL 12 or newer)

BEGIN;

CREATE SCHEMA IF NOT EXISTS kwtsms;

-- Settings read by the caps trigger. Change with: UPDATE kwtsms.settings SET value = '965,966' WHERE key = 'allowed_countries';
CREATE TABLE IF NOT EXISTS kwtsms.settings (
    key   text PRIMARY KEY,
    value text NOT NULL
);
INSERT INTO kwtsms.settings (key, value) VALUES
    ('allowed_countries',   '965'),   -- comma separated country prefixes
    ('max_per_number_hour', '5'),     -- queued messages per number per rolling hour
    ('max_daily_messages',  '1000')   -- queued messages in total per rolling 24 hours
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS kwtsms.outbox (
    id              bigserial PRIMARY KEY,
    mobile          text        NOT NULL,
    message         text        NOT NULL,
    sender          text,
    status          text        NOT NULL DEFAULT 'queued',
    test            boolean     NOT NULL DEFAULT true,
    msg_id          text,
    points          numeric(12,4),
    error_code      text,
    error_text      text,
    attempts        integer     NOT NULL DEFAULT 0,
    locked_at       timestamptz,
    locked_by       text,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    sent_at         timestamptz,
    CONSTRAINT outbox_status_chk CHECK (status IN ('queued', 'sending', 'sent', 'failed', 'unknown')),
    CONSTRAINT outbox_mobile_chk CHECK (mobile ~ '^[0-9]{7,15}$'),
    CONSTRAINT outbox_message_chk CHECK (length(btrim(message)) > 0)
);

CREATE INDEX IF NOT EXISTS outbox_claim_idx   ON kwtsms.outbox (next_attempt_at, id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS outbox_sending_idx ON kwtsms.outbox (locked_at) WHERE status = 'sending';
CREATE INDEX IF NOT EXISTS outbox_mobile_idx  ON kwtsms.outbox (mobile, created_at);
CREATE INDEX IF NOT EXISTS outbox_created_idx ON kwtsms.outbox (created_at);

-- Digits only: Arabic-Indic and Persian digits become 0-9, everything else that is not a digit is dropped
-- (+, spaces, dashes, brackets), leading 00 / 0 are removed, and the Kuwait trunk zero (9650...) is removed.
CREATE OR REPLACE FUNCTION kwtsms.normalize_mobile(p_mobile text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT regexp_replace(
               ltrim(
                   regexp_replace(
                       translate(p_mobile,
                                 E'٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹',
                                 '01234567890123456789'),
                       '[^0-9]', '', 'g'),
                   '0'),
               '^9650+', '965')
$$;

CREATE OR REPLACE FUNCTION kwtsms.setting(p_key text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT value FROM kwtsms.settings WHERE key = p_key
$$;

-- BEFORE INSERT: normalize, then enforce the country allowlist and the caps. The format CHECK runs after this trigger.
CREATE OR REPLACE FUNCTION kwtsms.outbox_before_insert() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = kwtsms, pg_temp AS $$
DECLARE
    v_allowed text;
    v_ok      boolean := false;
    v_prefix  text;
    v_hour    integer;
    v_day     integer;
    v_cnt     bigint;
BEGIN
    NEW.mobile := kwtsms.normalize_mobile(NEW.mobile);
    IF NEW.mobile IS NULL OR NEW.mobile !~ '^[0-9]{7,15}$' THEN
        RETURN NEW;  -- the CHECK constraint refuses it with a clear message
    END IF;

    v_allowed := coalesce(kwtsms.setting('allowed_countries'), '965');
    FOREACH v_prefix IN ARRAY string_to_array(replace(v_allowed, ' ', ''), ',') LOOP
        IF v_prefix <> '' AND left(NEW.mobile, length(v_prefix)) = v_prefix THEN
            v_ok := true;
            EXIT;
        END IF;
    END LOOP;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'kwtsms: country not allowed for number % (allowed: %)', NEW.mobile, v_allowed
            USING ERRCODE = 'KW001';
    END IF;

    v_hour := coalesce(kwtsms.setting('max_per_number_hour')::integer, 5);
    v_day  := coalesce(kwtsms.setting('max_daily_messages')::integer, 1000);

    -- one writer per number at a time, so two sessions cannot both slip under the cap
    PERFORM pg_advisory_xact_lock(hashtext('kwtsms:' || NEW.mobile));

    SELECT count(*) INTO v_cnt FROM kwtsms.outbox
     WHERE mobile = NEW.mobile AND created_at > now() - interval '1 hour';
    IF v_cnt >= v_hour THEN
        RAISE EXCEPTION 'kwtsms: hourly cap reached for number % (% per hour)', NEW.mobile, v_hour
            USING ERRCODE = 'KW002';
    END IF;

    SELECT count(*) INTO v_cnt FROM kwtsms.outbox WHERE created_at > now() - interval '24 hours';
    IF v_cnt >= v_day THEN
        RAISE EXCEPTION 'kwtsms: daily cap reached (% messages per 24 hours)', v_day
            USING ERRCODE = 'KW003';
    END IF;

    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION kwtsms.outbox_after_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('kwtsms_outbox', NEW.id::text);
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS outbox_before_insert ON kwtsms.outbox;
CREATE TRIGGER outbox_before_insert BEFORE INSERT ON kwtsms.outbox
    FOR EACH ROW EXECUTE FUNCTION kwtsms.outbox_before_insert();

DROP TRIGGER IF EXISTS outbox_after_insert ON kwtsms.outbox;
CREATE TRIGGER outbox_after_insert AFTER INSERT ON kwtsms.outbox
    FOR EACH ROW EXECUTE FUNCTION kwtsms.outbox_after_insert();

-- Queue one SMS. test defaults to true: nothing is delivered until you pass false AND the worker runs with KWTSMS_LIVE=1.
CREATE OR REPLACE FUNCTION kwtsms.enqueue(p_mobile text, p_message text, p_sender text DEFAULT NULL, p_test boolean DEFAULT true)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = kwtsms, pg_temp AS $$
DECLARE
    v_id bigint;
BEGIN
    INSERT INTO kwtsms.outbox (mobile, message, sender, test)
    VALUES (p_mobile, p_message, nullif(btrim(p_sender), ''), p_test)
    RETURNING id INTO v_id;
    RETURN v_id;
END
$$;

-- Rows stuck in sending (worker died mid-request) have an unknown outcome: mark them unknown, never resend.
CREATE OR REPLACE FUNCTION kwtsms.reap(p_older_than interval DEFAULT interval '10 minutes') RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = kwtsms, pg_temp AS $$
DECLARE
    v_n bigint;
BEGIN
    UPDATE kwtsms.outbox
       SET status = 'unknown',
           error_code = 'REAPED',
           error_text = 'worker stopped while sending; outcome unknown, not resent'
     WHERE status = 'sending' AND locked_at < now() - p_older_than;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END
$$;

-- Operator action: queue a failed or unknown row again. For unknown rows check the kwtSMS dashboard first, a resend can double send.
CREATE OR REPLACE FUNCTION kwtsms.requeue(p_id bigint) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = kwtsms, pg_temp AS $$
DECLARE
    v_n bigint;
BEGIN
    UPDATE kwtsms.outbox
       SET status = 'queued', attempts = 0, error_code = NULL, error_text = NULL,
           locked_at = NULL, locked_by = NULL, next_attempt_at = now()
     WHERE id = p_id AND status IN ('failed', 'unknown');
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
        PERFORM pg_notify('kwtsms_outbox', p_id::text);
    END IF;
    RETURN v_n > 0;
END
$$;

-- Delete finished rows older than the interval. Keep at least 2 days or the caps lose their history.
CREATE OR REPLACE FUNCTION kwtsms.purge(p_older_than interval DEFAULT interval '90 days') RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = kwtsms, pg_temp AS $$
DECLARE
    v_n bigint;
BEGIN
    DELETE FROM kwtsms.outbox
     WHERE created_at < now() - p_older_than AND status IN ('sent', 'failed', 'unknown');
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END
$$;

CREATE OR REPLACE VIEW kwtsms.outbox_stats AS
SELECT status,
       count(*)        AS messages,
       sum(points)     AS points,
       min(created_at) AS oldest,
       max(created_at) AS newest
  FROM kwtsms.outbox
 GROUP BY status;

-- Only the owner calls these by default; grant what you need (see README).
REVOKE ALL ON FUNCTION kwtsms.enqueue(text, text, text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION kwtsms.reap(interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION kwtsms.requeue(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION kwtsms.purge(interval) FROM PUBLIC;

COMMIT;
