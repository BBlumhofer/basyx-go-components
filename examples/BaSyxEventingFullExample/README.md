# BaSyx Go Full Eventing Example

A complete, interactive BaSyx environment that emits CloudEvents to **MQTT and
Kafka** on every Create / Update / Delete. Drop your shells and submodels into
[`./aas`](./aas), run `docker compose up`, and watch the events flow.

For the transport design see
[../../docu/developer/eventing_concept.md](../../docu/developer/eventing_concept.md),
and for a single-service starter see [../BaSyxEventingExample](../BaSyxEventingExample).

## Topology

| Service | Port | Eventing | Purpose |
| --- | --- | --- | --- |
| aas-environment | 8081 | ✅ | AAS Repository + Submodel Repository APIs, `./aas` import, upload; emits `aas`, `submodel`, `aas-descriptor`, `submodel-descriptor` events |
| aas-registry | 8082 | ✅ | AAS descriptor registry API |
| submodel-registry | 8083 | ✅ | Submodel descriptor registry API |
| aas-discovery | 8084 | ➖ | Asset-link discovery API (eventing not yet supported here) |
| digital-twin-registry | 8085 | ✅ | Combined AAS Registry + Discovery API (descriptor events only — see open discovery-eventing work) |
| Mosquitto | 1883 | — | MQTT broker |
| Redpanda | 9092 | — | Kafka broker |
| PostgreSQL | — | — | Shared model store + transactional outbox |
| basyx_configuration | — | — | Installs the schema incl. outbox patch `v1.2.0` |

All services share one PostgreSQL database. `aas-environment` has the registry and
discovery integrations enabled, so importing or writing a shell/submodel also
writes its descriptors and asset links — producing descriptor events too.

## Prerequisites

1. Build and publish the images with the **Eventing Images (GHCR)** GitHub Actions
   workflow (it builds every service used here, including `basyx-discovery-go`).
2. Make the GHCR packages pullable (public, or `docker login ghcr.io`). See the
   [run guide](../../docu/user/eventing_run_guide.md#2-build-and-publish-the-images-github-actions--ghcr).

The compose file defaults to `ghcr.io/bblumhofer/...:eventing`, so no extra setup
is needed for this fork. To use a different registry namespace or tag, either
`export IMAGE_OWNER=<owner>` (and optionally `IMAGE_TAG`) or copy
[`.env.example`](./.env.example) to `.env` and edit it.

## 1. Drop your models

Put any number of `.json`, `.xml`, or `.aasx` files into [`./aas`](./aas). Each is
imported on startup. A sample [`aas/sample-environment.json`](./aas/sample-environment.json)
with one shell and one submodel is included — delete it if you only want your own.

## 2. Start the environment

```sh
cd examples/BaSyxEventingFullExample
docker compose up -d
docker compose logs -f aas-environment   # wait for: EVENTING-RELAY-START ... sinks=[mqtt kafka]
```

On startup the models in `./aas` are imported, so you immediately see a burst of
`created` events.

## 3. Watch the events

```sh
# MQTT — all topics
mosquitto_sub -h localhost -t 'basyx/#' -v

# Kafka — the events topic
docker exec -it eventing_full_redpanda rpk topic consume basyx.events
```

Topics follow `basyx/<component>/<resource>/<operation>`, e.g.:

```
basyx/aas-environment/aas/created
basyx/aas-environment/submodel/created
basyx/aas-environment/aas-descriptor/created
basyx/aas-environment/submodel-descriptor/created
```

## 4. Interact

Endpoints (all share the same model store):

| API | Base URL |
| --- | --- |
| Shells (AAS Repository) | `http://localhost:8081/shells` |
| Submodels (Submodel Repository) | `http://localhost:8081/submodels` |
| Upload AASX/JSON/XML | `http://localhost:8081/upload` |
| AAS descriptors | `http://localhost:8082/shell-descriptors` |
| Submodel descriptors | `http://localhost:8083/submodel-descriptors` |
| Asset-link discovery | `http://localhost:8084/lookup/shells` |
| Digital Twin Registry — AAS descriptors | `http://localhost:8085/shell-descriptors` |

Create a submodel (emits `basyx/aas-environment/submodel/created`):

```sh
SM_ID="https://example.com/ids/sm/runtime-demo"
curl -sS -X POST http://localhost:8081/submodels \
  -H 'Content-Type: application/json' \
  -d '{"id":"'"$SM_ID"'","idShort":"RuntimeDemo","modelType":"Submodel",
       "submodelElements":[{"idShort":"Speed","modelType":"Property","valueType":"xs:double","value":"10"}]}'
```

Create a shell (emits `basyx/aas-environment/aas/created` and descriptor events):

```sh
AAS_ID="https://example.com/ids/aas/runtime-demo"
curl -sS -X POST http://localhost:8081/shells \
  -H 'Content-Type: application/json' \
  -d '{"id":"'"$AAS_ID"'","idShort":"RuntimeDemoAAS","modelType":"AssetAdministrationShell",
       "assetInformation":{"assetKind":"Instance","globalAssetId":"https://example.com/ids/asset/runtime-demo"}}'
```

Update / delete (identifiers are base64url-encoded in the path):

```sh
# Portable base64url (Linux + macOS):
SM_ID_B64=$(printf '%s' "$SM_ID" | base64 | tr '+/' '-_' | tr -d '=')

curl -sS -X PUT "http://localhost:8081/submodels/$SM_ID_B64" \
  -H 'Content-Type: application/json' \
  -d '{"id":"'"$SM_ID"'","idShort":"RuntimeDemo","modelType":"Submodel",
       "submodelElements":[{"idShort":"Speed","modelType":"Property","valueType":"xs:double","value":"42"}]}'   # -> submodel/updated

curl -sS -X DELETE "http://localhost:8081/submodels/$SM_ID_B64"                                                  # -> submodel/deleted
```

Each Kafka record on `basyx.events` carries the same CloudEvents JSON keyed by the
entity identifier (so all events for one entity stay ordered on one partition).

## 5. Reset for a fresh import

This compose keeps no named database volume, so a full teardown re-imports `./aas`
from scratch on the next start:

```sh
docker compose down    # drops the database
docker compose up -d   # re-installs schema and re-imports ./aas
```

## Notes & troubleshooting

- **A service exits on startup with an eventing error.** The MQTT/Kafka sinks
  connect eagerly; ensure `mosquitto` and `redpanda` are healthy first
  (`docker compose ps`). The compose file already waits on their health.
- **`aas-discovery` produces no events.** Discovery does not yet route writes
  through the eventing mutation seam; it is included for a complete environment.
  Shell/submodel and descriptor events are unaffected.
- **No Kafka records.** The `redpanda-init` service creates `basyx.events` on
  startup. If it is missing, create it manually:
  `docker exec -it eventing_full_redpanda rpk topic create basyx.events`.
- **Schema version mismatch.** Every service requires schema `v1.2.0`; the
  configuration service installs it first. All service images must be built from
  the eventing branch (the workflow does this, including discovery).
- **Inspect the outbox** (e.g. during a broker outage):
  `docker exec -it eventing_full_postgres psql -U admin -d basyxTestDB -c "SELECT component, operation, status, pending_sinks, attempts FROM event_outbox ORDER BY id DESC LIMIT 20;"`
