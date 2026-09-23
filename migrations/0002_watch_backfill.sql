-- A watch can be registered after payments already reached its address (the caller was slow to
-- register it). Such a watch starts before the durable cursor; `backfill_to` is the cursor at
-- registration, and blocks [start_block, backfill_to] -- swept before the watch existed -- are
-- scanned once for it by the next sweep. NULL when nothing needs scanning, or once it is done.
ALTER TABLE watches ADD COLUMN backfill_to BIGINT;
CREATE INDEX watches_backfill_idx ON watches (chain_id, seq) WHERE backfill_to IS NOT NULL;
