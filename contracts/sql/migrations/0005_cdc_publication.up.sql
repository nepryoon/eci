-- ADR-0020: the connector is not the database owner. The owner creates the
-- fixed publication; the dedicated replication role receives only the table
-- read privilege needed by Debezium. CNPG manages the role itself.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_publication
    WHERE pubname = 'eci_outbox_publication'
  ) THEN
    EXECUTE 'CREATE PUBLICATION eci_outbox_publication FOR TABLE public.outbox';
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'eci_cdc'
  ) THEN
    GRANT SELECT ON TABLE public.outbox TO eci_cdc;
  END IF;
END
$$;
