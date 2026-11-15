-- FILE:      sql/uninstall.sql
-- CREATED:   2026-11-16 02:05 +03
-- TASK:      int-postgres
-- AUTHOR:    M Alhassoun
-- WHAT:      Removes the kwtsms schema and everything in it, including every queued and sent row.
-- WHY:       Clean removal. Export kwtsms.outbox first if you need the history.
-- USAGE:     psql "$DATABASE_URL" -f sql/uninstall.sql

DROP SCHEMA IF EXISTS kwtsms CASCADE;
