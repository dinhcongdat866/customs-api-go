-- Review queue: "oldest needs_review first, page by page".
-- Composite index, leftmost-prefix rule: the query filters on status (equality)
-- then orders by (created_at, id), so the index is read in order with no sort.
-- id is the tie-breaker that makes the keyset cursor unambiguous.
SET lock_timeout = '10s';

CREATE INDEX IF NOT EXISTS shipments_status_created_idx
  ON goapi.shipments (status, created_at, id);
