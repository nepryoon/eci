-- A rollback cannot preserve consumer-scoped rows when more than one consumer
-- has processed the same event. Fail closed instead of deleting provenance.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM processed_events
    GROUP BY event_id
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION
      'cannot restore global processed_events key: event_id exists for multiple consumers';
  END IF;
END
$$;

ALTER TABLE processed_events
  DROP CONSTRAINT processed_events_pkey;

ALTER TABLE processed_events
  ADD CONSTRAINT processed_events_pkey PRIMARY KEY (event_id);
