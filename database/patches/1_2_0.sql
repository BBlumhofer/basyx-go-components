-- ============================================================================
-- Project        : Eclipse BaSyx
-- Organization   : Fraunhofer IESE
-- File Type      : SQL Patch Script
-- Patch Version  : 1.2.0
-- Metamodel Ver. : 3.2
-- ----------------------------------------------------------------------------
-- Description:
--   Transactional outbox for CUD eventing. The outbox row is written inside the
--   same transaction as the model mutation so publication is atomic with the
--   business write, while the actual broker delivery (MQTT/Kafka) happens
--   asynchronously in a relay after commit. A separate per-entity sequence
--   table provides a gap-detectable monotonic order that survives outbox
--   cleanup.
--
-- Copyright (c) Eclipse BaSyx Authors and Fraunhofer IESE
-- SPDX-License-Identifier: MIT
-- ============================================================================

-- Durable, gap-detectable per-entity event sequence. Kept independent of the
-- outbox rows so published-row cleanup never resets the exposed order.
CREATE TABLE IF NOT EXISTS event_outbox_sequence (
  component         TEXT NOT NULL,
  identifier_digest CHAR(64) NOT NULL,
  identifier        TEXT NOT NULL,
  last_sequence     BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
  db_updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (component, identifier_digest)
);

-- One row per logical resource mutation. payload holds the complete, rendered
-- CloudEvents 1.0 envelope, frozen at enqueue time so publish retries are
-- byte-identical (idempotent for consumers via the stable event_id).
CREATE TABLE IF NOT EXISTS event_outbox (
  id                BIGSERIAL PRIMARY KEY,
  event_id          UUID NOT NULL,
  component         TEXT NOT NULL,
  entity_type       TEXT NOT NULL,
  identifier        TEXT NOT NULL,
  identifier_digest CHAR(64) NOT NULL,
  operation         TEXT NOT NULL CHECK (operation IN ('created', 'updated', 'deleted')),
  entity_sequence   BIGINT NOT NULL CHECK (entity_sequence > 0),
  occurred_at       TIMESTAMPTZ NOT NULL,
  correlation_id    TEXT,
  request_id        TEXT,
  actor_subject     TEXT,
  topic             TEXT NOT NULL,
  partition_key     TEXT NOT NULL,
  content_type      TEXT NOT NULL DEFAULT 'application/cloudevents+json',
  payload           JSONB NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'published', 'dead')),
  pending_sinks     TEXT NOT NULL DEFAULT '',
  attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error        TEXT,
  published_at      TIMESTAMPTZ,
  db_created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (event_id),
  UNIQUE (component, identifier_digest, entity_sequence)
);

-- Claim path: only due, undelivered rows, ordered by insertion for global FIFO.
CREATE INDEX IF NOT EXISTS ix_event_outbox_claim
  ON event_outbox (next_attempt_at, id)
  WHERE status = 'pending';

-- Per-entity ordered scan used while a single entity is drained under its lock.
CREATE INDEX IF NOT EXISTS ix_event_outbox_entity
  ON event_outbox (identifier_digest, id)
  WHERE status = 'pending';

-- Bounded cleanup of delivered rows.
CREATE INDEX IF NOT EXISTS ix_event_outbox_cleanup
  ON event_outbox (published_at)
  WHERE status = 'published';

UPDATE basyxsystem
SET schema_version = 'v1.2.0',
    state = 'clean'
WHERE identifier = (
  SELECT identifier
  FROM basyxsystem
  ORDER BY identifier ASC
  LIMIT 1
);
