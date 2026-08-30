-- ADR-0020: the connector is not the database owner. The owner creates the
-- fixed publication; the passwordless privilege carrier receives only the
-- table read privilege needed by Debezium. CNPG manages the carrier and adds
-- the dedicated login role as a member when CDC is enabled. This preserves the
-- grant across a supported disabled -> enabled chart upgrade.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_publication
    WHERE pubname = 'eci_outbox_publication'
  ) THEN
    EXECUTE 'CREATE PUBLICATION eci_outbox_publication FOR TABLE public.outbox';
  END IF;

  -- Compose/testcontainer do not have the CNPG role reconciler and retain the
  -- existing owner-based CDC development path. In Kubernetes the carrier is
  -- always present, including cdc.enabled=false, so this grant is applied
  -- before any later login-role enablement.
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'eci_cdc_outbox_reader'
  ) THEN
    GRANT SELECT ON TABLE public.outbox TO eci_cdc_outbox_reader;
  END IF;
END
$$;
