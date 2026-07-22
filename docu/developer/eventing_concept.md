# Eventing Concept: Transactional CUD Event Publishing (MQTT + Kafka)

Status: Concept / Design proposal
Scope: Issue #188 — Eventing for CUD Operations
Related: PR #502 (history-independent WORM evidence), `internal/common/history`

---

## 1. Goal and Scope

Emit an event whenever an in-scope resource is **created, updated, or deleted**, so
external systems can react to model lifecycle changes in near real time. Delivery
must be reliable (at-least-once), ordered per entity, and must never affect the
correctness or latency contract of the write API.

The issue mandates MQTT as the first transport. This concept designs a
**transport-independent** pipeline and delivers **two production transports at
once — MQTT and Kafka** — behind one shared abstraction, so an operator can enable
either or both.

In-scope components (unchanged from the issue):

- AAS Registry, Submodel Registry, Discovery
- AAS Repository, Submodel Repository, Concept Description Repository
- Registry of Infrastructures

Non-goals: inbound command handling, event replay/consumer tooling, and event-driven
sourcing of the model. Eventing is **write-notification only**.

Guiding constraints (from the issue, preserved here):

- Disabled by default.
- Independent from `history` and from WORM `evidence` configuration.
- One normalized mutation record per logical resource mutation.
- No events for reads or rolled-back transactions.
- Persistence code contains no transport-specific logic.

---

## 2. What already exists in the codebase

This concept is not greenfield. The repository already contains the exact seams
required, most of them added alongside the WORM evidence work in PR #502.

| Asset | Location | Role in this concept |
| --- | --- | --- |
| Reserved config namespace `eventing.*` (`enabled`, `format`, `sinks`, `outboxEnabled`, `topicPrefix`) | `internal/common/configuration.go` (`EventingConfig`, L316) | Extended, no breaking change |
| Fail-fast guard `validateEventingConfig` "not implemented yet" | `internal/common/configuration.go:936` | Replaced by real validation |
| `ChangeEvent` (CloudEvents-ready: entity, operation, snapshot, actor, correlation) | `internal/common/history/types.go:119` | Source of the mutation record |
| `EventPublisher` interface, reserved for "future CloudEvents-compatible publishing" | `internal/common/history/types.go:166` | Realized by the outbox enqueue |
| Single in-transaction CUD choke point `AppendVersionTx` / `AppendMutatedVersionTx` | `internal/common/history/append.go:75` | Where events are captured |
| Per-entity advisory lock `LockMutationTx` | `internal/common/history/append.go` | Reused for per-entity ordering |
| Per-mutation WORM chain (sequence + hash chain, in-tx) | `internal/common/history/mutation_evidence.go` | Structural precedent for the outbox |
| Process-local `Configure()` / `ActiveConfig()` pattern | `internal/common/history/config.go` | Copied for `eventing.Configure()` |
| Audit/correlation propagation via context | `internal/common/history/audit.go` | Supplies CloudEvents correlation fields |
| Pluggable `EvidenceStore` interface + S3 impl | `internal/common/history/evidence_types.go`, `evidence_store_s3.go` | Model for the `EventSink` interface |
| Schema patch mechanism (versioned, advisory-locked, clean/dirty) | `database/patches/`, `cmd/basyxconfigurationservice/main.go`, `internal/common/database.go` | Adds the outbox table |

**Key architectural fact:** the WORM evidence writer publishes *synchronously
inside the model transaction*. That is correct for tamper-evident evidence, but it
is the wrong model for eventing, where a broker outage must never roll back a
business write. Eventing therefore introduces a genuine **transactional outbox**:
commit a durable record in the model transaction, publish asynchronously after
commit. This is the one net-new infrastructure piece.

---

## 3. Architecture Overview

Four layers, strictly separated. Layers 1–2 run inside the model transaction;
layers 3–4 run asynchronously.

```
   HTTP CUD request
        │
        ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  Model transaction  (common.ExecuteInTransaction / *sql.Tx)  │
 │                                                              │
 │   Layer 1  Mutation capture                                  │
 │     history.AppendVersionTx(...)  ── already called by all   │
 │     in-scope repositories ── produces a ChangeEvent.         │
 │                                                              │
 │   Layer 2  Transactional outbox                              │
 │     eventing.EnqueueTx(ctx, tx, MutationRecord)              │
 │       INSERT INTO event_outbox (...)   -- same tx            │
 │                                                              │
 │   COMMIT  ── model rows + outbox row are atomic              │
 └─────────────────────────────────────────────────────────────┘
        │ (commit succeeds → API returns 2xx immediately)
        ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  Relay / dispatcher   (background goroutine, per instance)   │
 │                                                              │
 │   Layer 3  Claim pending rows (per-entity, ordered)          │
 │     SELECT ... FOR UPDATE SKIP LOCKED + advisory lock        │
 │     build CloudEvents 1.0 envelope                           │
 │                                                              │
 │   Layer 4  Publish to configured sinks                       │
 │     EventSink.Publish(ctx, envelope)                         │
 │        ├── mqttSink   (Eclipse Paho, MQTT v5, QoS 1)         │
 │        └── kafkaSink  (franz-go, idempotent producer)        │
 │     mark published / retry with bounded backoff / dead-letter│
 └─────────────────────────────────────────────────────────────┘
```

### Why this shape

- **Atomicity without coupling:** the outbox row is written in the same
  `*sql.Tx` as the model change (Layer 2), so a rollback discards the event and a
  broker outage cannot roll back a write. This is the defining property of the
  transactional outbox pattern.
- **Reuse of the existing choke point:** every in-scope repository already funnels
  every CUD through `AppendVersionTx`. Capturing events there means zero new event
  logic scattered across handlers, satisfying "persistence code avoids duplicated
  transport-specific logic".
- **Transport independence:** the outbox stores a fully-rendered, transport-neutral
  CloudEvents envelope. Sinks are pure delivery adapters selected by config.

---

## 4. Layer 1 — Normalized Mutation Record

One `MutationRecord` per logical resource mutation. It is derived from the
`ChangeEvent` that `AppendVersionTx` already builds, so no new capture call sites
are introduced. The record is transport-neutral and complete.

```go
// internal/common/eventing/record.go
type MutationRecord struct {
    EventID       uuid.UUID     // stable CloudEvents id, reused on every retry
    Component     string        // e.g. "submodel-repository"
    EntityType    string        // "aas" | "submodel" | "concept-description" | "aas-descriptor" | ...
    Identifier    string        // resource identifier
    Operation     Operation     // Created | Updated | Deleted
    EntitySequence int64        // per-entity monotonic order (chain head)
    OccurredAt    time.Time     // domain event time
    Resource      map[string]any // complete persisted resource for Created/Updated
    Deletion      *DeletionRef   // documented deletion representation for Deleted
    Correlation   Correlation    // CorrelationID, RequestID, actor subject/issuer
}
```

Content rules (acceptance criteria):

- **Create / Update** carry the complete persisted resource (`ChangeEvent.Snapshot`).
- **Delete** carries a documented deletion representation: `{ "id": <identifier>,
  "entityType": <type>, "deleted": true }`. Full pre-delete state is intentionally
  *not* included here — it is the domain of WORM evidence, which stays independent.
- `EventID` is a UUID generated once at enqueue time and persisted, so
  publish retries always reuse the same id → idempotent consumers.
- `EntitySequence` is taken from the per-entity chain head already maintained under
  the advisory lock `LockMutationTx` holds, guaranteeing a gap-free monotonic order
  per entity.

`EntityType`/`Component` are derived from the `history.Table*` constant and the
service identity, so the mapping is centralized and consistent.

---

## 5. Layer 2 — Transactional Outbox

### 5.1 Schema (new patch `database/patches/1_2_0.sql`)

Modeled on `mutation_evidence_artifacts` (sequence + digest + JSONB payload), with
added delivery-state columns for the relay.

```sql
CREATE TABLE IF NOT EXISTS event_outbox (
  id                BIGSERIAL PRIMARY KEY,
  event_id          UUID NOT NULL,
  component         TEXT NOT NULL,
  entity_type       TEXT NOT NULL,
  identifier        TEXT NOT NULL,
  identifier_digest CHAR(64) NOT NULL,                 -- sha256(identifier) for indexing + hash routing
  operation         TEXT NOT NULL CHECK (operation IN ('created','updated','deleted')),
  entity_sequence   BIGINT NOT NULL CHECK (entity_sequence > 0),
  occurred_at       TIMESTAMPTZ NOT NULL,
  correlation_id    TEXT,
  request_id        TEXT,
  actor_subject     TEXT,
  topic             TEXT NOT NULL,                      -- resolved hierarchical topic
  partition_key     TEXT NOT NULL,                      -- ordering key (default = identifier)
  content_type      TEXT NOT NULL DEFAULT 'application/cloudevents+json',
  payload           JSONB NOT NULL,                     -- complete CloudEvents 1.0 envelope
  status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','inflight','published','dead')),
  pending_sinks     TEXT[] NOT NULL,                    -- sinks not yet acked; empties on full delivery
  attempts          INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error        TEXT,
  published_at      TIMESTAMPTZ,
  db_created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (event_id),
  UNIQUE (component, identifier_digest, entity_sequence)
);

-- Claim path: only unfinished rows that are due.
CREATE INDEX IF NOT EXISTS ix_event_outbox_claim
  ON event_outbox (next_attempt_at, id)
  WHERE status IN ('pending','inflight');

-- Per-entity ordered scan.
CREATE INDEX IF NOT EXISTS ix_event_outbox_entity
  ON event_outbox (identifier_digest, entity_sequence)
  WHERE status IN ('pending','inflight');

-- Bounded cleanup of delivered rows.
CREATE INDEX IF NOT EXISTS ix_event_outbox_cleanup
  ON event_outbox (published_at)
  WHERE status = 'published';
```

Registration follows the established rule (per `AGENTS.md`):

1. Add `database/patches/1_2_0.sql` with the standard banner header.
2. Register in `cmd/basyxconfigurationservice/main.go` as the new final patch bound
   to `common.CURRENT_DATABASE_VERSION`.
3. Bump `CURRENT_DATABASE_VERSION` to `"v1.2.0"` in `internal/common/database.go`.

`pending_sinks` avoids a second delivery table while still supporting multiple
transports: a row is only `published` once every required sink has acked, and a
retry re-sends only to sinks still listed — so a Kafka outage does not re-deliver to
MQTT.

### 5.2 Enqueue (GOQU, in the model transaction)

```go
// internal/common/eventing/outbox.go
func EnqueueTx(ctx context.Context, tx *sql.Tx, rec MutationRecord) error {
    cfg := ActiveConfig()
    if !cfg.Enabled || !cfg.OutboxEnabled {
        return nil // disabled ⇒ zero overhead, no row written
    }
    envelope, err := BuildCloudEvent(cfg, rec) // structured JSON, see §8
    if err != nil {
        return err
    }
    insert, args, err := goqu.Dialect(common.Dialect).
        Insert("event_outbox").
        Rows(goqu.Record{
            "event_id":          rec.EventID,
            "component":         rec.Component,
            "entity_type":       rec.EntityType,
            "identifier":        rec.Identifier,
            "identifier_digest": sha256Hex(rec.Identifier),
            "operation":         rec.Operation.String(),
            "entity_sequence":   rec.EntitySequence,
            "occurred_at":       rec.OccurredAt,
            "correlation_id":    rec.Correlation.CorrelationID,
            "request_id":        rec.Correlation.RequestID,
            "actor_subject":     rec.Correlation.ActorSubject,
            "topic":             ResolveTopic(cfg, rec),
            "partition_key":     rec.Identifier,
            "payload":           goqu.L("?::jsonb", envelope),
            "pending_sinks":     goqu.L("?", pq.Array(cfg.EnabledSinks())),
        }).ToSQL()
    if err != nil {
        return common.NewInternalServerError("EVENTING-ENQUEUE-BUILD " + err.Error())
    }
    if _, err = tx.ExecContext(ctx, insert, args...); err != nil {
        return common.NewInternalServerError("EVENTING-ENQUEUE-EXEC " + err.Error())
    }
    return nil
}
```

Integration point: `AppendVersionTx` calls `eventing.EnqueueTx` after appending the
history/evidence rows, using the same `tx`, the same advisory lock, and the same
`ChangeEvent`. Because both the history path and the eventing path are individually
gated (`history.Mode`, `EvidenceEnabled`, `eventing.Enabled`), the three features are
fully independent — eventing works with history fully off, matching the WORM
evidence precedent.

**Failure semantics (acceptance criteria):**

- Enqueue happens inside the model tx ⇒ if it fails, the whole mutation rolls back.
  "Outbox failure rolls back eventing-enabled mutations." ✔
- A rolled-back tx discards the outbox row ⇒ "no events for rolled-back
  transactions." ✔
- Reads never call `AppendVersionTx` ⇒ "no events for reads." ✔

---

## 6. Layer 3 — Relay / Dispatcher

A background goroutine started once per service process in `cmd/<service>/main.go`,
after the DB connection and alongside `history.Configure(...)`, gated on
`cfg.Eventing.OutboxEnabled`. It uses the existing cancelable root context from
`common.SignalContext()` for graceful shutdown.

### 6.1 Ordering-preserving claim

Per-entity ordering must hold across worker goroutines **and** across service
replicas that share the database. The claim is therefore grouped by entity and
guarded by a PostgreSQL advisory lock so only one relay processes a given entity at
a time:

```
loop (every eventing.relay.pollIntervalMs, backpressure-aware):
  BEGIN
    -- pick due entities, newest-first fairness, skip those a peer holds
    candidates := SELECT DISTINCT identifier_digest
                  FROM event_outbox
                  WHERE status IN ('pending','inflight') AND next_attempt_at <= now()
                  ORDER BY min(next_attempt_at)
                  LIMIT relay.entityBatch
    for each digest in candidates:
        if not pg_try_advisory_xact_lock(hashtext(digest)): continue   -- peer owns it
        rows := SELECT * FROM event_outbox
                WHERE identifier_digest = digest AND status IN ('pending','inflight')
                ORDER BY entity_sequence            -- strict per-entity order
                LIMIT relay.perEntityBatch
        publish rows in order (§6.2)
  COMMIT   -- releases advisory locks
```

Because entities are independent, many entities publish in parallel (high
throughput), while each individual entity is strictly serialized (correct order).
This scales horizontally: N replicas partition the entity space automatically via
the advisory lock, with no leader election.

### 6.2 Publish, retry, dead-letter

For each claimed row, in `entity_sequence` order:

1. Deserialize the stored CloudEvents envelope (no re-rendering — the payload is
   frozen at enqueue time, so retries are byte-identical and idempotent).
2. For each sink still in `pending_sinks`, call `EventSink.Publish`. On ack, drop it
   from `pending_sinks`.
3. If `pending_sinks` becomes empty → `status='published'`, `published_at=now()`.
4. On any sink error → `attempts++`, `next_attempt_at = now() + backoff(attempts)`,
   record `last_error`, **stop advancing this entity** (later sequences wait, so
   order is never broken by a transient failure).
5. When `attempts >= eventing.relay.maxAttempts` → `status='dead'` (dead-letter,
   inspected via metrics/queries; the entity's later events remain blocked to
   preserve order until an operator intervenes, which is the safe default for an
   ordered stream).

Backoff is bounded exponential with jitter:
`min(base * 2^attempts, maxBackoff) ± jitter`, defaults `base=1s`, `max=60s`.

Delivery guarantee: **at-least-once**. A crash between broker ack and the
status update re-publishes on restart; the stable `EventID` lets consumers dedupe.

---

## 7. Layer 4 — Sink abstraction (MQTT + Kafka)

One interface, mirroring the existing `EvidenceStore` shape, keeps the relay
transport-agnostic:

```go
// internal/common/eventing/sink.go
type Envelope struct {
    Topic        string            // hierarchical topic / Kafka topic
    PartitionKey string            // ordering key (entity identifier)
    ContentType  string            // application/cloudevents+json
    Body         []byte            // structured CloudEvents JSON
    Headers      map[string]string // ce-id, ce-source, ce-type, traceparent ...
}

type EventSink interface {
    Name() string
    Publish(ctx context.Context, e Envelope) error // returns nil only on broker ack
    Close(ctx context.Context) error
}
```

`eventing.sinks: ["mqtt", "kafka"]` selects which adapters are constructed at
startup. Adding a future transport (AMQP, NATS, HTTP webhook) is a new `EventSink`
implementation and a config block — no change to layers 1–3.

### 7.1 MQTT sink

- Library: **`github.com/eclipse/paho.golang/autopaho`** (Eclipse Paho, MQTT v5,
  built-in auto-reconnect and session resumption). MQTT v5 gives per-message
  content-type and user properties, which map cleanly onto CloudEvents attributes.
  (`paho.mqtt.golang` v3.1.1 is the fallback if v5 brokers are not available.)
- Topic: `<topicPrefix>/<component>/<resource>/<operation>`, e.g.
  `basyx/submodel-repository/submodel/updated`. Built by `ResolveTopic`.
- QoS: default **1** (at-least-once), configurable. `retained`: default **false**,
  configurable.
- CloudEvents mode: **structured** — the whole envelope is the MQTT payload with
  content type `application/cloudevents+json`. Binary mode (attributes in MQTT v5
  user properties) is an optional follow-up.
- Ordering caveat: MQTT ordering is per-topic and best-effort. The relay guarantees
  ordered *submission* per entity (single in-flight per entity via the advisory
  lock), which yields correct order for a single broker connection with QoS 1. This
  caveat is documented for operators; Kafka is the choice where strict ordering is a
  hard requirement.

### 7.2 Kafka sink

- Library: **`github.com/twmb/franz-go`** — pure Go (no CGo/librdkafka), high
  throughput, first-class **idempotent producer** and transactions.
  (`segmentio/kafka-go` is a simpler fallback; `confluent-kafka-go` is the CGo/
  librdkafka option if an operator standardizes on it.)
- Topic: configurable mapping. Default single topic `basyx.events` with the
  hierarchical topic string carried as a CloudEvents attribute + Kafka header;
  optional per-component topics `basyx.<component>` for coarse routing.
- Partition key: the **entity identifier**. Kafka guarantees per-key ordering within
  a partition, so per-entity order is preserved natively even under high concurrency.
  This is the strong-ordering transport.
- Producer config: `acks=all`, idempotent producer enabled, `max.in.flight` bounded,
  compression (lz4/zstd) configurable. The idempotent producer suppresses duplicates
  from in-session retries; cross-restart at-least-once is still deduped by consumers
  via `EventID`.
- CloudEvents mode: structured JSON in the record value; `ce_*` headers set for
  binary-mode consumers.

---

## 8. CloudEvents 1.0 envelope

Structured JSON envelope (issue mandate), rendered once at enqueue time by
`BuildCloudEvent` using **`github.com/cloudevents/sdk-go/v2`** for the `event.Event`
type and canonical JSON marshalling. Rendering at enqueue (not at publish) keeps
retries byte-identical and keeps the transport layer free of business mapping.

| CloudEvents attribute | Source |
| --- | --- |
| `specversion` | `"1.0"` |
| `id` | `MutationRecord.EventID` (stable across retries) |
| `source` | `<externalUrl or component>` (URI-reference) |
| `type` | `org.eclipse.basyx.<resource>.<operation>` e.g. `org.eclipse.basyx.submodel.updated` |
| `subject` | resource identifier |
| `time` | `OccurredAt` (RFC 3339) |
| `datacontenttype` | `application/json` |
| `data` | complete resource (create/update) or deletion representation (delete) |
| extension `basyxcomponent` | component name |
| extension `basyxsequence` | per-entity sequence |
| extension `correlationid` | correlation id from audit context |
| extension `traceparent` | W3C trace context when present |

The `eventing.format` field keeps the door open for alternative envelopes; today the
only supported value is `cloudevents`, validated at startup.

---

## 9. Delivery guarantees summary

| Property | Mechanism | Guarantee |
| --- | --- | --- |
| Atomic with model write | outbox INSERT in model `*sql.Tx` | Exactly the committed set is eventable |
| No event on rollback/read | in-tx enqueue; reads never enqueue | ✔ |
| At-least-once | ack-before-mark + retry | ✔ |
| Idempotent retry | frozen envelope + stable `EventID` | Duplicate-safe for consumers |
| Per-entity ordering | advisory lock per entity + `entity_sequence` scan; Kafka key = identifier | ✔ (strict on Kafka; ordered-submission on MQTT) |
| Broker outage isolation | async relay, backoff, dead-letter | API latency unaffected |
| Horizontal scale | advisory-lock entity partitioning | No leader election needed |

---

## 10. Configuration

Extends the reserved `EventingConfig` (`internal/common/configuration.go`) without
breaking the existing shape. New nested `mqtt`, `kafka`, `relay`, and `retention`
blocks follow the `HistoryEvidenceConfig` precedent.

```yaml
eventing:
  enabled: false                 # master switch (default off)
  format: cloudevents            # only supported value today
  topicPrefix: basyx
  outboxEnabled: false           # required for any delivery; gates the in-tx enqueue
  sinks: []                      # e.g. ["mqtt"], ["kafka"], or ["mqtt","kafka"]

  relay:
    pollIntervalMs: 250
    entityBatch: 128             # distinct entities claimed per cycle
    perEntityBatch: 64           # rows drained per entity per cycle
    maxAttempts: 12
    backoffBaseMs: 1000
    backoffMaxMs: 60000

  retention:
    publishedTtlHours: 168       # bounded cleanup of delivered rows (7 days)
    cleanupIntervalMin: 30
    deadLetterTtlHours: 720      # dead rows retained 30 days for inspection

  mqtt:
    brokerUrl: "tls://broker:8883"
    clientId: "basyx-submodel-repo"
    qos: 1
    retained: false
    username: ""
    password: ""
    tls:
      caPath: ""
      certPath: ""
      keyPath: ""

  kafka:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    topic: "basyx.events"        # or per-component when topicPerComponent=true
    topicPerComponent: false
    acks: all
    idempotent: true
    compression: lz4
    tls:
      caPath: ""
      certPath: ""
      keyPath: ""
    sasl:
      mechanism: ""              # "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
      username: ""
      password: ""
```

Environment overrides extend the existing `BASYX_EVENTING_*` set
(`applyEventingEnvOverrides`), e.g. `BASYX_EVENTING_MQTT_BROKER_URL`,
`BASYX_EVENTING_KAFKA_BROKERS`, `BASYX_EVENTING_SINKS=mqtt,kafka`.

Startup activation (new `eventing.Configure` + `eventing.StartRelay`), placed next to
the existing history wiring in each `cmd/<service>/main.go`:

```go
history.Configure(...)                         // existing
if err := history.ConfigureEvidence(ctx, ...); // existing
    err != nil { return err }

if err := eventing.Configure(cfg.Eventing); err != nil { return err }  // new
relay, err := eventing.StartRelay(ctx, db, cfg.Eventing)               // new, gated inside
if err != nil { return err }
defer relay.Shutdown(shutdownCtx)
```

**Validation** replaces the current `CONFIG-EVENTING-NOTIMPLEMENTED` guard
(`configuration.go:936`) with real checks, mirroring
`validateHistoryEvidenceConfig`: `enabled ⇒ outboxEnabled` and at least one sink;
each named sink must have a valid config block; `format == cloudevents`; TLS/SASL
field completeness.

---

## 11. Observability

Metrics (Prometheus-style; the exact registry follows whatever the services already
expose) required by the acceptance criteria:

| Metric | Type | Meaning |
| --- | --- | --- |
| `eventing_outbox_pending` | gauge | queue depth (status pending/inflight) |
| `eventing_publish_total{sink,operation,result}` | counter | publish attempts / successes / failures |
| `eventing_publish_latency_seconds{sink}` | histogram | broker publish latency |
| `eventing_retry_total{sink}` | counter | retries performed |
| `eventing_dead_letter_total{component}` | counter | dead-lettered events |
| `eventing_relay_cycle_seconds` | histogram | relay loop duration (backpressure signal) |
| `eventing_oldest_pending_seconds` | gauge | age of the oldest undelivered event (SLA alarm) |

Structured logs use the existing coded-error convention
(`EVENTING-<FN>-<STEP>`). Correlation id and event id are logged on every publish
outcome for traceability.

---

## 12. Retention / cleanup

A second lightweight background loop (or a piggybacked pass in the relay) enforces
the documented, bounded cleanup policy:

- Delete `status='published'` rows older than `retention.publishedTtlHours`.
- Retain `status='dead'` rows for `retention.deadLetterTtlHours` for inspection,
  then delete.
- Deletes are batched (`DELETE ... WHERE id IN (SELECT ... LIMIT n)`) to avoid long
  locks, mirroring the bounded-work approach used elsewhere.

This satisfies "documented bounded outbox cleanup policy" and prevents unbounded
table growth on high-write deployments.

---

## 13. Security

- Broker credentials, TLS, and SASL come only from config/env; never logged.
- Event payloads reuse the model the API already returned to the caller; no
  additional data exposure. Deletion events carry only the identifier.
- Actor/correlation attributes are copied from the already-sanitized audit context;
  no new PII surface.
- `context.Background()` is not used in live code — the relay derives its context
  from `common.SignalContext()`, and per-cycle work uses request/relay contexts, per
  the repository's security rule that context must carry security information.

---

## 14. Performance

- **Zero cost when disabled:** `EnqueueTx` returns immediately if eventing/outbox is
  off; no row, no allocation.
- **One extra INSERT per mutation when enabled:** a single indexed JSONB insert in a
  transaction that already writes history/model rows — negligible relative to
  existing work, and it never contacts the broker on the request path.
- **Async fan-out:** broker latency and outages are fully decoupled from API
  latency.
- **Batched, parallel relay:** per-entity parallelism scales with entity cardinality;
  Kafka batching + compression sustains high throughput; MQTT uses a persistent
  session.
- **Bounded growth:** claim/cleanup indexes are partial, keeping the hot working set
  small even as the historical `published` set is trimmed.

---

## 15. Testing strategy

Integration tests (in `integration_tests/`, following the repository convention of
running the full suite) must cover every acceptance criterion:

1. **CUD coverage:** each implemented create/update/delete in each in-scope component
   produces exactly one outbox row with the correct type/topic/payload.
2. **No spurious events:** GET requests and rolled-back transactions produce no rows
   (inject a post-enqueue error, assert rollback discards the outbox row).
3. **Independence matrix:** history off / evidence off / eventing on, and all other
   combinations, behave correctly.
4. **Retry:** a failing stub sink causes bounded retries with backoff, then success
   marks `published`.
5. **Deduplication:** a crash between ack and status update re-publishes the same
   `EventID`; the stub consumer sees a duplicate id it can drop.
6. **Ordering:** concurrent updates to one entity are delivered in `entity_sequence`
   order; concurrent updates to different entities may interleave.
7. **Dead-letter:** exceeding `maxAttempts` moves the row to `dead` and blocks that
   entity's later events until cleared.
8. **Cleanup:** delivered rows past TTL are removed; dead rows are retained per
   policy.

Sinks are tested against embedded/dev brokers (e.g. a test MQTT broker container and
a single-broker Kafka container) plus in-memory stub sinks for deterministic
retry/ordering/dedup tests.

---

## 16. Rollout plan (incremental, each phase shippable)

1. **Foundation:** `internal/common/eventing` package — `MutationRecord`,
   `EnqueueTx`, outbox table patch `1_2_0.sql`, config expansion + real validation,
   `eventing.Configure`. No sinks yet; `outboxEnabled` can be exercised with a no-op
   sink and asserted purely at the DB level. (Fully testable, no brokers.)
2. **Relay + CloudEvents:** dispatcher goroutine, envelope builder, retry/backoff,
   dead-letter, metrics, cleanup. Ships with an in-memory sink.
3. **MQTT sink:** Paho v5 adapter, topics/QoS/retained/TLS, ordering caveat docs.
4. **Kafka sink:** franz-go adapter, keying/partitions, idempotent producer, TLS/SASL.
5. **Component rollout:** wire `eventing.StartRelay` into each in-scope service
   `main.go`; add per-component integration tests.

Phases 3 and 4 are independent and can land in either order or in parallel, since
both implement the same `EventSink` interface.

---

## 17. Mapping to acceptance criteria

| Acceptance criterion (issue #188) | Satisfied by |
| --- | --- |
| Events for every implemented CUD in all in-scope components | §4 capture at `AppendVersionTx`, §16 phase 5 |
| No events for reads or rolled-back transactions | §5.2 in-tx enqueue |
| History, evidence, eventing independently configurable | §5.2 independent gates, §10 |
| One normalized mutation record per logical mutation | §4 `MutationRecord`, `UNIQUE(component,identifier_digest,entity_sequence)` |
| Persistence code avoids duplicated transport logic | §3 single choke point, §7 sink abstraction |
| Outbox failure rolls back eventing-enabled mutations | §5.2 failure semantics |
| At-least-once with idempotent retry via stable event IDs | §6.2, §8 `EventID` |
| Ordering preserved per entity | §6.1 advisory-lock claim, §7.2 Kafka keying |
| Complete resources on create/update; documented deletion representation | §4 content rules |
| Metrics: publish failures, retry state, queue depth, dead-lettered | §11 |
| Documented bounded outbox cleanup policy | §12 |
| Comprehensive integration tests (CUD, rollback, retry, dedup, ordering) | §15 |
| MQTT transport | §7.1 |
| **Kafka transport (extension requested for this concept)** | §7.2 |

---

## 18. New dependencies

| Purpose | Module | Notes |
| --- | --- | --- |
| CloudEvents envelope | `github.com/cloudevents/sdk-go/v2` | Envelope type + canonical JSON only; transport handled by sinks |
| MQTT | `github.com/eclipse/paho.golang` (autopaho, MQTT v5) | Eclipse project; v3 `paho.mqtt.golang` fallback |
| Kafka | `github.com/twmb/franz-go` | Pure Go, idempotent producer; `segmentio/kafka-go` / `confluent-kafka-go` alternatives |
| UUID | `github.com/google/uuid` | Stable event ids |

All are permissively licensed (Apache-2.0 / MIT / EPL-2.0), compatible with the
project's MIT licensing and Eclipse provenance.
