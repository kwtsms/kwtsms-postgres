# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub: Security tab, "Report a vulnerability" on this repository.
Do not open a public issue for a vulnerability. We acknowledge reports within a few days.

For account or billing questions, contact kwtSMS support at https://www.kwtsms.com.

## Good practice for this project

- Run the worker as the least-privilege role in `sql/worker_role.sql`.
- Keep `KWTSMS_PASSWORD` and `DATABASE_URL` in the environment or a secret store, never in the database or in git.
- Grant `EXECUTE` on `kwtsms.enqueue` only to the roles that need to send.
- The worker starts in test mode: nothing is delivered until you set `KWTSMS_LIVE=1` and queue rows with `test = false`.
