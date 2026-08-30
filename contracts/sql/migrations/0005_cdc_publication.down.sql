DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'eci_cdc_outbox_reader'
  ) THEN
    REVOKE SELECT ON TABLE public.outbox FROM eci_cdc_outbox_reader;
  END IF;
END
$$;
DROP PUBLICATION IF EXISTS eci_outbox_publication;
