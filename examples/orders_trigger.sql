-- FILE:      examples/orders_trigger.sql
-- CREATED:   2026-11-16 02:50 +03
-- TASK:      int-postgres
-- AUTHOR:    M Alhassoun
-- WHAT:      Example: an orders table whose AFTER INSERT trigger queues an SMS through kwtsms.enqueue().
-- WHY:       Shows the one-line integration: the SMS is part of the same transaction as the order. If the order
--            rolls back, no SMS is queued.
-- USAGE:     psql "$DATABASE_URL" -f examples/orders_trigger.sql   (after sql/install.sql)

CREATE TABLE IF NOT EXISTS public.orders (
    id       bigserial PRIMARY KEY,
    customer text NOT NULL,
    mobile   text NOT NULL,            -- any format: +965..., 00965..., spaces, Arabic digits
    total    numeric(10,3) NOT NULL,
    created  timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.orders_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- p_test defaults to true: the row is queued in test mode (nothing is delivered).
    -- Pass false as the 4th argument, and run the worker with KWTSMS_LIVE=1, to deliver for real.
    PERFORM kwtsms.enqueue(
        NEW.mobile,
        'Thank you ' || NEW.customer || ', order #' || NEW.id || ' received. Total ' || NEW.total || ' KWD.'
    );
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS orders_notify ON public.orders;
CREATE TRIGGER orders_notify AFTER INSERT ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.orders_notify();

-- Try it (the number is a placeholder: use your own):
--   INSERT INTO public.orders (customer, mobile, total) VALUES ('Sara', '965XXXXXXXX', 12.500);
--   SELECT id, mobile, status, test, error_code FROM kwtsms.outbox ORDER BY id DESC LIMIT 5;
