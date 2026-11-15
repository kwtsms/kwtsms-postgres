-- FILE:      sql/worker_role.sql
-- CREATED:   2026-11-16 02:05 +03
-- TASK:      int-postgres
-- AUTHOR:    M Alhassoun
-- WHAT:      Creates the least-privilege login role the worker runs as.
-- WHY:       The worker needs to read and update the outbox and call reap(); it needs nothing else.
-- USAGE:     psql "$DATABASE_URL" -f sql/worker_role.sql   (then: ALTER ROLE kwtsms_worker PASSWORD '...')

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kwtsms_worker') THEN
        CREATE ROLE kwtsms_worker LOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA kwtsms TO kwtsms_worker;
GRANT SELECT, UPDATE ON kwtsms.outbox TO kwtsms_worker;
GRANT EXECUTE ON FUNCTION kwtsms.reap(interval) TO kwtsms_worker;
