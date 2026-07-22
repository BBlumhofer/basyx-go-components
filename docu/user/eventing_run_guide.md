# Eventing Run Guide (MQTT + Kafka)

This guide walks you through building the eventing-enabled BaSyx Go images,
publishing them to your fork's GitHub Container Registry (GHCR), and running a
local stack that emits CloudEvents to both MQTT and Kafka on every Create,
Update, and Delete. It is meant to be copy-pasteable end to end.

Design background: [eventing_concept.md](../developer/eventing_concept.md).

> Want the full multi-service environment (AAS + Submodel repositories,
> registries, discovery) with a drop-in `aas/` folder for shells and submodels?
> See [examples/BaSyxEventingFullExample](../../examples/BaSyxEventingFullExample).
> This guide covers the single-service starter.

---

## 1. Prerequisites

- A fork of `basyx-go-components` with GitHub Actions enabled (Actions tab →
  "I understand my workflows, go ahead and enable them" if prompted).
- Docker with Compose v2 (`docker compose ...`).
- `curl` and `jq` for driving the API and reading responses.

The stack it runs:

| Component | Image | Purpose |
| --- | --- | --- |
| PostgreSQL | `postgres:18` | Model store + transactional outbox |
| Mosquitto | `eclipse-mosquitto:2` | MQTT broker |
| Redpanda | `redpandadata/redpanda` | Kafka broker (Kafka API compatible) |
| Configuration service | `ghcr.io/<owner>/basyx-configuration-go` | Installs schema incl. outbox patch `v1.2.0` |
| Submodel Repository | `ghcr.io/<owner>/basyx-submodelrepository-go` | Emits events on CUD |

---

## 2. Build and publish the images (GitHub Actions → GHCR)

The workflow `.github/workflows/eventing-images.yml` builds the configuration
service and every eventing-enabled service and pushes them to
`ghcr.io/<owner>/basyx-<service>-go`. It uses the built-in `GITHUB_TOKEN`, so no
registry secrets are needed.

1. Push this branch to your fork (already done if you are reading this there), or
   open the **Actions** tab → **Eventing Images (GHCR)** → **Run workflow**.
2. Wait for the matrix jobs to finish (~5–10 min with cache). Each job publishes:
   - `ghcr.io/<owner>/basyx-configuration-go:eventing`
   - `ghcr.io/<owner>/basyx-submodelrepository-go:eventing`
   - `ghcr.io/<owner>/basyx-aasrepository-go:eventing`
   - `ghcr.io/<owner>/basyx-conceptdescriptionrepository-go:eventing`
   - `ghcr.io/<owner>/basyx-aasregistry-go:eventing`
   - `ghcr.io/<owner>/basyx-submodelregistry-go:eventing`
   - `ghcr.io/<owner>/basyx-aasenvironment-go:eventing`
   - (each also tagged with the short commit SHA and the branch name)

### Make the images pullable

New GHCR packages are **private** by default. Either:

- **Make them public** (simplest for testing): GitHub → your profile → **Packages**
  → open each `basyx-*-go` package → **Package settings** → **Change visibility** →
  Public. Or
- **Authenticate the Docker pull**:
  ```sh
  echo <YOUR_GITHUB_PAT_with_read:packages> | docker login ghcr.io -u <owner> --password-stdin
  ```

> Building locally instead of GHCR? See [§8](#8-alternative-build-images-locally).

---

## 3. Configure the example

The compose file defaults to `ghcr.io/bblumhofer/...:eventing`, so on this fork
no configuration is needed. To point at a different registry namespace or tag:

```sh
cd examples/BaSyxEventingExample
export IMAGE_OWNER=<your-github-owner-lowercase>   # optional
export IMAGE_TAG=eventing                          # optional
# or: cp .env.example .env && $EDITOR .env
```

---

## 4. Start the stack

```sh
docker compose up -d
docker compose logs -f submodel-repository
```

You are ready when the submodel repository log shows the relay start line:

```
EVENTING-RELAY-START component=submodel-repository sinks=[mqtt kafka]
```

If the service exits immediately, the most common cause is that a broker was not
reachable at startup (the sinks connect eagerly). Check `docker compose ps` and
the `mosquitto` / `redpanda` health, then `docker compose up -d` again.

---

## 5. Watch the events

Open two terminals.

**MQTT** — subscribe to every BaSyx topic:

```sh
docker run --rm -it --network basyxeventingexample_default eclipse-mosquitto:2 \
  mosquitto_sub -h mosquitto -t 'basyx/#' -v
```

> The compose network is named `<dir>_default`; confirm with `docker network ls`.
> Alternatively, since port 1883 is published: `mosquitto_sub -h localhost -t 'basyx/#' -v`.

**Kafka** — consume the events topic:

```sh
docker exec -it eventing_redpanda rpk topic consume basyx.events
```

---

## 6. Trigger Create / Update / Delete

Create a submodel:

```sh
SM_ID="https://example.com/ids/sm/eventing-demo"

curl -sS -X POST http://localhost:8081/submodels \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "'"$SM_ID"'",
    "idShort": "EventingDemo",
    "modelType": "Submodel",
    "submodelElements": [
      { "idShort": "Temperature", "modelType": "Property", "valueType": "xs:double", "value": "21.5" }
    ]
  }' | jq .
```

Update it (the identifier is base64url-encoded in the path):

```sh
# Portable base64url (works on Linux and macOS):
SM_ID_B64=$(printf '%s' "$SM_ID" | base64 | tr '+/' '-_' | tr -d '=')

curl -sS -X PUT "http://localhost:8081/submodels/$SM_ID_B64" \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "'"$SM_ID"'",
    "idShort": "EventingDemo",
    "modelType": "Submodel",
    "submodelElements": [
      { "idShort": "Temperature", "modelType": "Property", "valueType": "xs:double", "value": "42.0" }
    ]
  }'
```

Delete it:

```sh
curl -sS -X DELETE "http://localhost:8081/submodels/$SM_ID_B64"
```

### What you should see

On the MQTT subscriber:

```
basyx/submodel-repository/submodel/created {"specversion":"1.0","id":"...","source":"...","type":"org.eclipse.basyx.submodel.created","subject":"https://example.com/ids/sm/eventing-demo",...}
basyx/submodel-repository/submodel/updated {...,"type":"org.eclipse.basyx.submodel.updated",...}
basyx/submodel-repository/submodel/deleted {...,"type":"org.eclipse.basyx.submodel.deleted",...}
```

Each Kafka record on `basyx.events` carries the same CloudEvents JSON as its
value, keyed by the submodel identifier (so all events for one entity land on the
same partition and stay ordered).

A CloudEvents envelope looks like:

```json
{
  "specversion": "1.0",
  "id": "0f7c...-uuid",
  "source": "http://localhost:8081",
  "type": "org.eclipse.basyx.submodel.created",
  "subject": "https://example.com/ids/sm/eventing-demo",
  "time": "2026-07-22T10:15:03Z",
  "datacontenttype": "application/json",
  "basyxcomponent": "submodel-repository",
  "basyxresource": "submodel",
  "basyxsequence": 1,
  "data": { "id": "https://example.com/ids/sm/eventing-demo", "modelType": "Submodel", "submodelElements": [ ... ] }
}
```

---

## 7. Configuration reference

All settings live under `eventing.*` in the service config, and each has an
environment-variable form. The example sets these on the `submodel-repository`
service; the same variables work on every eventing-enabled service.

| Env var | Default | Meaning |
| --- | --- | --- |
| `BASYX_EVENTING_ENABLED` | `false` | Master switch |
| `BASYX_EVENTING_OUTBOX_ENABLED` | `false` | Required when enabled; writes the in-tx outbox row |
| `BASYX_EVENTING_FORMAT` | `cloudevents` | Envelope format (only value today) |
| `BASYX_EVENTING_TOPIC_PREFIX` | `basyx` | First topic segment |
| `BASYX_EVENTING_SINKS` | — | Comma-separated: `mqtt`, `kafka`, or both |
| `BASYX_EVENTING_MQTT_BROKER_URL` | — | e.g. `mqtt://host:1883` or `tls://host:8883` |
| `BASYX_EVENTING_MQTT_CLIENT_ID` | — | MQTT client id |
| `BASYX_EVENTING_KAFKA_BROKERS` | — | Comma-separated `host:port` seed brokers |
| `BASYX_EVENTING_KAFKA_TOPIC` | `basyx.events` | Kafka topic |

Additional YAML-only tuning (defaults shown) — set via a mounted `config.yaml`
`eventing:` block; see the commented example in
`cmd/submodelrepositoryservice/config.yaml`:

- `eventing.mqtt.qos: 1`, `eventing.mqtt.retained: false`, `eventing.mqtt.tls.*`
- `eventing.kafka.acks: all`, `eventing.kafka.idempotent: true`,
  `eventing.kafka.compression: lz4`, `eventing.kafka.topicPerComponent: false`,
  `eventing.kafka.tls.*`, `eventing.kafka.sasl.*`
- `eventing.relay.pollIntervalMs: 250`, `maxAttempts: 12`,
  `backoffBaseMs: 1000`, `backoffMaxMs: 60000`, `entityBatch`, `perEntityBatch`
- `eventing.retention.publishedTtlHours: 168`, `deadLetterTtlHours: 720`,
  `cleanupIntervalMin: 30`

Topics are `basyx/<component>/<resource>/<operation>`, e.g.
`basyx/submodel-repository/submodel/updated`.

---

## 8. Verify reliability under a broker outage

The whole point of the transactional outbox is that broker downtime never breaks
or blocks writes. To see it:

```sh
# 1. Stop the MQTT broker.
docker compose stop mosquitto

# 2. Create/update a few submodels — the API keeps returning 2xx.
#    (Events stay durably queued in the outbox; Kafka still receives them.)
curl -sS -X POST http://localhost:8081/submodels -H 'Content-Type: application/json' \
  -d '{"id":"urn:sm:outage-1","idShort":"Outage1","modelType":"Submodel"}'

# 3. Inspect the queued rows.
docker exec -it eventing_postgres psql -U admin -d basyxTestDB \
  -c "SELECT operation, pending_sinks, attempts, status FROM event_outbox ORDER BY id;"

# 4. Bring MQTT back — the relay drains the backlog automatically.
docker compose start mosquitto
```

You will see `pending_sinks` shrink to empty and rows move to `status=published`
once delivery succeeds. Rows that exhaust `maxAttempts` become `status=dead`
(dead-letter) and are retained for inspection per the retention policy.

---

## 9. Alternative: build images locally

If you prefer not to use GHCR, build each image from the repo root and retag it
into the `ghcr.io/${IMAGE_OWNER}/...` names the compose file expects (or edit the
compose file to point at your local tags):

```sh
docker build -f cmd/basyxconfigurationservice/Dockerfile \
  -t ghcr.io/local/basyx-configuration-go:eventing .
docker build -f cmd/submodelrepositoryservice/Dockerfile \
  -t ghcr.io/local/basyx-submodelrepository-go:eventing .
# ...repeat for the other services you need...
```

Then set `IMAGE_OWNER=local` in `.env` and `docker compose up -d`.

---

## 10. Troubleshooting

- **Service exits on startup with an eventing error.** The MQTT/Kafka sinks
  connect eagerly and fail fast if a broker is unreachable. Ensure `mosquitto`
  and `redpanda` are healthy (`docker compose ps`) before the repository starts;
  the compose file already declares `depends_on ... condition: service_healthy`.
- **`CONFIG-EVENTING-...` at startup.** Configuration validation rejected the
  eventing settings — e.g. `enabled` without `outboxEnabled`, no sinks, or a sink
  missing its broker. The error code names the exact field.
- **Schema version mismatch on service start.** The configuration service must run
  first and reach `v1.2.0` (it installs `database/patches/1_2_0.sql`). Confirm with
  `docker compose logs basyx_configuration` and
  `docker exec -it eventing_postgres psql -U admin -d basyxTestDB -c "SELECT schema_version,state FROM basyxsystem;"`.
- **No Kafka records.** The `redpanda-init` service creates `basyx.events` on
  startup. If it is missing, create it with
  `docker exec -it eventing_redpanda rpk topic create basyx.events`.
- **`docker pull` denied for ghcr.io.** The package is private; make it public or
  `docker login ghcr.io` (see §2).
