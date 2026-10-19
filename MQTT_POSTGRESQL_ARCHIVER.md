# MQTT-to-PostgreSQL archiver notes

These notes capture an initial design discussion for a possible service that
subscribes to MQTT telemetry, cleans payloads according to their topic, and
archives them in PostgreSQL.

## Proposed data flow

```text
lokimankeli -> Mosquitto -> Redpanda Connect -> PostgreSQL
```

Redpanda Connect is the leading implementation candidate. It can run locally
as a standalone process or container and does not require a Redpanda or Kafka
broker. Its MQTT input, conditional processors, jq processor, batching, and SQL
output cover most of the desired behavior without a custom daemon.

PostgreSQL is a suitable archive for moderate, append-oriented telemetry when
the records must remain queryable. Store the fields commonly used for lookup
as ordinary columns and retain the cleaned, device-specific document as
JSONB. A starting schema would contain:

- an archive row identifier;
- the event timestamp from the payload;
- a separate database receipt timestamp;
- the MQTT topic;
- an optional stable event identifier for deduplication; and
- the cleaned payload as JSONB.

Start with a B-tree index on `(topic, event_at DESC)`. Add expression or GIN
indexes for JSON fields only after real query patterns identify which ones are
worth indexing. If messages have stable event identifiers, a unique constraint
such as `(topic, event_id)` can make at-least-once delivery idempotent.

JSONB preserves the document structure and values but not the exact source
bytes, whitespace, or object-key order. If byte-for-byte retention ever
matters, store the original payload separately.

## Topic-specific payload cleanup

Each MQTT topic or topic pattern should select a jq filter before the message
is inserted. Typical filters could remove Fahrenheit readings when Celsius is
also present, retain only cumulative energy counters, or remove sensitive and
otherwise redundant fields while preserving the rest of the original object.

Prefer MQTT topic-filter syntax (`home/+/temperature`, `power/#`) for ordinary
routing because it matches the MQTT model and is easier to review than regular
expressions. Add regular-expression matching only if MQTT filters prove
insufficient. If a custom archiver is built, a reasonable initial policy is:

- routes are evaluated in configuration order;
- the first matching route wins;
- an explicit catch-all route stores messages that should remain unchanged;
- no jq output means an intentional discard; and
- more than one jq output is rejected, preserving one MQTT event to one
  database row.

Redpanda Connect can instead use a `switch` processor whose conditions inspect
the `mqtt_topic` message metadata. Each branch can run its own jq processor.
The jq processor uses Go's `gojq` implementation rather than the jq executable,
so common operations such as `del`, `select`, object construction, traversal,
and conditionals work, but implementation differences should be checked when
using unusual jq features.

Compile or validate all filters before starting message consumption. A runtime
filter error must never silently store the original payload: doing so could
retain data that the filter was intended to remove. Log the failure and route
the event to an explicit dead-letter destination, or stop/retry according to a
chosen poison-message policy.

## Delivery and failure behavior

MQTT QoS 1 and an at-least-once database pipeline can deliver duplicates.
Prefer a stable event identifier supplied by the publisher and enforce its
uniqueness in PostgreSQL. Without one, document that duplicates are possible;
hash-based deduplication may incorrectly merge legitimate identical readings.

Use a stable MQTT client identifier and a persistent broker session if events
must queue while the archiver is briefly offline. Confirm the MQTT protocol
version and session-expiry settings together rather than relying only on a
`clean_session` default.

Database insertion should be batched for efficiency. Retries need bounded
backoff and visible logging. Permanent failures, including malformed JSON and
filter errors, need a dead-letter or explicit discard policy so one poison
message cannot block the pipeline forever.

Useful operational signals include:

- MQTT connection and reconnection state;
- messages received, filtered, inserted, retried, and dead-lettered;
- insertion latency and outstanding batch size;
- readiness of both the MQTT input and PostgreSQL output; and
- the age of the newest successfully archived event.

## Redpanda Connect deployment

Redpanda Connect is distinct from the Redpanda Kafka-compatible broker. The
Connect component is a standalone stream-processing executable and is
available as a native binary and as the official
`docker.redpanda.com/redpandadata/connect` container image.

Most Redpanda Connect components and functionality are Apache-2.0 licensed.
Some enterprise-only components use the Redpanda Community License and require
a commercial license. The MQTT input, jq processing, and ordinary SQL output
needed for this pipeline are expected to use the free functionality; verify
the component catalog again when selecting and pinning a release.

The official distribution sends anonymized usage telemetry unless it is
disabled. Use `--disable-telemetry` if this is unwanted.

### Podman Quadlet

Running the pipeline as a Quadlet is a good fit for a host that already uses
Podman-managed services. The container should:

- use an explicitly pinned image version rather than `latest`;
- mount the pipeline YAML read-only;
- run `run --disable-telemetry /config/connect.yaml`;
- obtain MQTT and PostgreSQL credentials from Podman secrets or another
  protected, non-command-line source;
- restart on failure and write its logs to journald;
- optionally expose the default health/metrics HTTP listener only on
  localhost; and
- rely on reconnection and readiness behavior even when ordering it after
  local Mosquitto or PostgreSQL units.

An indicative Quadlet shape is:

```ini
[Unit]
Description=MQTT telemetry archiver
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.redpanda.com/redpandadata/connect:<pinned-version>
ContainerName=mqtt-archive
Volume=/etc/mqtt-archive/connect.yaml:/config/connect.yaml:ro,Z
Exec=run --disable-telemetry /config/connect.yaml
Secret=mqtt-archive-mqtt-password,type=env,target=MQTT_PASSWORD
Secret=mqtt-archive-postgres-password,type=env,target=POSTGRES_PASSWORD

[Service]
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

This is a design sketch, not yet a validated deployment file. In particular,
confirm the Quadlet secret syntax against the Podman version on the target
host. Environment-style secrets stay out of command-line arguments and the
Quadlet file, but are still present in the container process environment. A
protected mounted configuration or credential file may be preferable if the
selected connectors can consume it directly.

Keep jq filters and non-secret pipeline configuration in ordinary files so
they can be version-controlled and tested. Keep credentials outside version
control. Initially avoid automatic container-image updates so behavior does
not change without validating the new release and filters.

## Open design decisions

Before deploying the pipeline, decide the following:

1. Whether unmatched topics are ignored or stored unchanged. An explicit
   catch-all is preferable to an implicit behavior.
2. Which timestamp field is authoritative, its accepted format, and what to do
   when it is absent or invalid. Always retain a separate receipt timestamp.
3. Which payload field is the stable event ID, if any, and the exact database
   uniqueness constraint.
4. Whether a poison message is retried, dead-lettered, or discarded after a
   configured number of attempts.
5. Whether retained MQTT messages should be archived on every new session or
   recognized and handled separately.
6. Whether the Connect health/metrics endpoint should be enabled and, if so,
   how it is restricted and monitored.
7. Whether first-match routing is sufficient or overlapping routes should be
   rejected during configuration validation.

## References

- [Redpanda Connect overview](https://docs.redpanda.com/connect/get-started/about/)
- [Installation options](https://docs.redpanda.com/connect/install/)
- [Container quick start](https://docs.redpanda.com/connect/get-started/quickstarts/rpk/)
- [MQTT input and metadata](https://docs.redpanda.com/connect/components/inputs/mqtt/)
- [jq processor](https://docs.redpanda.com/connect/components/processors/jq/)
- [switch processor](https://docs.redpanda.com/connect/components/processors/switch/)
- [SQL insert output](https://docs.redpanda.com/connect/components/outputs/sql_insert/)
- [Configuration and validation](https://docs.redpanda.com/connect/configuration/about/)
- [Usage telemetry and disabling it](https://docs.redpanda.com/connect/configuration/telemetry/)
- [Redpanda Connect licensing FAQ](https://github.com/redpanda-data/connect/blob/main/licenses/README.md)
- [Enterprise feature licensing](https://docs.redpanda.com/connect/get-started/licensing/)
