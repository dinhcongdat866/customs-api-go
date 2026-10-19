-- Everything lives in its own schema so this service can share a Postgres
-- instance with the Next.js app without touching its tables.
SET lock_timeout = '10s';

CREATE SCHEMA IF NOT EXISTS goapi;

CREATE TABLE IF NOT EXISTS goapi.batches (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source          text NOT NULL,                 -- 'api' | 'csv'
  customer_id     text NOT NULL,
  idempotency_key text UNIQUE,                   -- client-chosen; same key => same batch
  item_count      integer NOT NULL DEFAULT 0,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS goapi.shipments (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id            uuid NOT NULL REFERENCES goapi.batches(id),
  external_ref        text,
  raw_title           text NOT NULL,
  raw_description     text,
  declared_value      numeric(12,2),             -- money: never float
  currency            text,
  origin_country      text,                      -- ISO-2
  destination_country text,                      -- ISO-2
  quantity            integer NOT NULL DEFAULT 1,
  material            text,
  hs_code             text,
  hs_confidence       double precision,
  status              text NOT NULL DEFAULT 'received'
                      CHECK (status IN ('received','processing','needs_review','needs_info','cleared','blocked','failed')),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shipments_status_idx ON goapi.shipments (status);
CREATE INDEX IF NOT EXISTS shipments_batch_idx  ON goapi.shipments (batch_id);

-- Every classification attempt is kept; never overwritten.
CREATE TABLE IF NOT EXISTS goapi.classifications (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  shipment_id    uuid NOT NULL REFERENCES goapi.shipments(id),
  candidates     jsonb NOT NULL,                 -- top-3, best first
  chosen_hs_code text,
  confidence     double precision,
  model          text NOT NULL,
  prompt_version text NOT NULL,
  latency_ms     integer,
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS classifications_shipment_idx ON goapi.classifications (shipment_id);

-- Append-only: who/what did what to a shipment, and why.
CREATE TABLE IF NOT EXISTS goapi.audit_log (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  shipment_id uuid NOT NULL REFERENCES goapi.shipments(id),
  actor       text NOT NULL,                     -- 'system:classifier' | 'user:dat'
  action      text NOT NULL,                     -- 'classified' | 'routed' | ...
  detail      jsonb,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_shipment_idx ON goapi.audit_log (shipment_id);
