-- Kafka fan-out delivers the same outbox event to independent consumer groups.
-- Deduplication is therefore scoped to the consumer, not globally to event_id.
ALTER TABLE processed_events
  DROP CONSTRAINT processed_events_pkey;

ALTER TABLE processed_events
  ADD CONSTRAINT processed_events_pkey PRIMARY KEY (event_id, consumer_name);
