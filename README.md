# lokimankeli

`lokimankeli` follows the stdout records of configured systemd services,
optionally decodes each `MESSAGE` as JSON, transforms it with jq, and publishes
it to MQTT.

This is basically cleaner implementation of the sender filters in my old tool
[systemdb](https://github.com/zouppen/systemdb/blob/master/send/examples/heppa).

## Message flow

Each `[[route]]` selects one systemd unit and defines a required
`publish_filter` jq program. One journal reader follows all routed units in the
configured journal scope and dispatches each record to the filter for its
unit. Unit names must be unique.

An optional `journal_match` table adds exact journal-field constraints to a
route. Fields are combined with AND; an array of values for one field is
combined with OR. With no constraints, all records attributed to the unit are
accepted regardless of transport. For example, a container using journald can
exclude other records associated with its service:

```toml
journal_match = { _TRANSPORT = "journal", SYSLOG_IDENTIFIER = "my-container" }
```

Each route may set `message_format = "json"` to decode `MESSAGE` before jq, or
`message_format = "string"` to pass the complete text directly. The default is
`json`, which accepts any JSON value, not only objects. Invalid JSON or invalid
UTF-8 text is warned, skipped, and checkpointed.

The filter receives that decoded value or string and emits zero or more MQTT
publication descriptors. Each result must contain a `topic` string and a
`payload` JSON value, and may contain a `strictness` override. This lets one
journal entry produce different payloads and delivery policies on different
topics.

The filter also receives `$timestamp_ms`, the journal receive time in Unix
milliseconds, and `$event_id`, an unpadded base64url HMAC-SHA256 of the journal
cursor. Multiple publications from one journal record share an event ID.

```jq
{
  topic: "victron/\(.address)/rssi",
  payload: {
    ts: $timestamp_ms,
    id: $event_id,
    rssi: .rssi
  }
},
{
  topic: "victron/\(.address)/data",
  payload: (
    .payload
    | del(.rssi)
    | .ts = $timestamp_ms
    | .id = $event_id
  )
}
```

The bridge collects and validates every descriptor before publishing any of
them. Topics must be valid MQTT publication topics and must differ from the
configured state topic. Payloads are serialized as compact UTF-8 JSON. A string
payload is therefore published as a JSON string, including its quotes. An
intentional `empty` or `select(...)` result publishes nothing and still
checkpoints the journal entry.

Telemetry uses QoS 1 and is not retained. The last acknowledged journal cursor
is written unencrypted as retained QoS 1 JSON to the configured state topic.
Only the bridge should have access to that topic.

The bridge requires MQTT 5 so it can inspect PUBACK reason codes. The required
`mqtt.strictness` setting controls broker-rejected telemetry:

- `ignore` silently skips it.
- `warn` logs a warning and skips it.
- `fail` exits without checkpointing the journal entry.
- `require-sub` also exits when the broker explicitly reports that no
  subscription matched the topic.

MQTT brokers are not required to report `No matching subscribers`, so the last
mode is a best-effort check rather than a delivery guarantee. Broker rejection
of the retained checkpoint is always fatal regardless of this setting.

Each jq output may override the configured policy for that publication with a
`strictness` field. Omit the field to inherit `mqtt.strictness`.

Each route's required `filter_strictness` setting controls jq evaluation errors
and invalid publication descriptors for that route's journal entries:

- `ignore` silently skips and checkpoints the entry.
- `warn` logs a warning, then skips and checkpoints the entry.
- `fail` exits without checkpointing the entry.

Jq compilation errors are always fatal. An intentional zero-output filter is a
successful result and checkpoints normally.

## Install

Python 3.11 or newer is required. On Debian, the native dependencies are
available as `python3-systemd`, `python3-paho-mqtt`, and `python3-jq`. When
installing dependencies from PyPI, building `systemd-python` may additionally
require `libsystemd-dev`, `pkg-config`, a compiler, and Python development
headers.

For example, install the project from the repository root with pip:

```console
python3 -m pip install .
```

Then copy [`examples/config.toml`](examples/config.toml) to a location of your
choice and restrict its permissions. The program deliberately has no default
config path:

```console
lokimankeli --config /path/to/bridge.toml
```

Set `[general] log_level = "debug"` to log each message's source unit, jq
routing decision, and MQTT acknowledgement outcome. The default is `info`.
Debug logs include topic names but not payloads, cursors, or event IDs.

The initial checkpoint and recovery procedure is described under
[Service deployment](#service-deployment).

The config file contains the MQTT password and the HMAC key configured as
`general.event_id_key`. Use mode `0600`. The system service reads its
root-owned configuration through a systemd credential; the user service reads
its configuration directly as the current user.

## Service deployment

Alternative system and user units are under [`systemd/`](systemd/). Select one
and normally run a single bridge instance, adding a `[[route]]` for every
service it should follow. Separate bridge instances require distinct MQTT
client IDs, state topics, and systemd state directories.

The system unit requires systemd 247 or newer and uses `DynamicUser`
with membership in `systemd-journal`; no persistent service account is
needed. Its root-owned configuration is loaded from
`/etc/lokimankeli.toml` into the protected credential
directory. Optional `LoadCredential` examples in the unit can place
TLS files in the same directory, allowing `ca_file`, `cert_file`, and
`key_file` to use the relative names shown in the example
configuration.

The user unit reads its configuration from
`%h/.config/lokimankeli/config.toml`. It reads only the current user journal
and should set `journal.scope = "user"`.

The selected unit creates a private `lokimankeli` state directory. A file named
`start-position` in that directory overrides retained MQTT state for the next
start. Its contents must be either `now` or one global journal cursor. `now`
means the newest entry in the configured system or user journal, without unit
or transport filters.

On the first system-service start, the missing checkpoint makes the process
fail and systemd schedules a retry. That first attempt also creates the state
directory. Supply the position before the next retry:

```console
sudo systemctl start lokimankeli.service
printf '%s\n' now | sudo tee /var/lib/lokimankeli/start-position >/dev/null
```

To recover from a purged checkpoint or deliberately replay from another
position, write the request and restart the running service:

```console
printf '%s\n' 's=...;i=...;b=...;m=...;t=...;x=...' \
  | sudo tee /var/lib/lokimankeli/start-position >/dev/null
sudo systemctl restart lokimankeli.service
```

Obtain cursors from `journalctl -o json`. Cursors are global within the
configured journal scope and need not belong to a routed unit.

For the user unit, obtain the state root with `systemd-path user-state`. As with
the system unit, the first failed start creates the directory and the automatic
retry consumes the request:

```console
systemctl --user start lokimankeli.service
user_state=$(systemd-path user-state)
printf '%s\n' now >"$user_state/lokimankeli/start-position"
```

Before saving `now`, the bridge replaces it atomically with the concrete tail
cursor. It removes `start-position` only after MQTT acknowledges the retained
checkpoint and before telemetry delivery starts. Once `now` has been resolved,
later failures leave the concrete, retryable request in place.

For a direct non-systemd invocation, set `STATE_DIRECTORY` to a private
directory containing the same `start-position` file before starting the bridge.

The MQTT identity needs permission to publish telemetry and read/write its
configured state topic. Telemetry-only consumers should be denied access to
the state topic. Enable TLS whenever the broker connection crosses an
untrusted network.

## Failure semantics

All routes share one cursor. It advances only after all publications derived
from the current journal entry are acknowledged. A fatal route or publication
error therefore prevents later records from every routed unit from being
processed until the error is corrected. A crash between telemetry
acknowledgement and checkpoint acknowledgement may replay the complete output
group; `$event_id` remains stable. Jq runtime failures and invalid publication
descriptors are handled according to that route's `filter_strictness`. An
intentional zero-output result checkpoints normally. Invalid message encoding,
invalid JSON in JSON-mode routes, and invalid journal timestamps are warned,
skipped, and checkpointed.

With `mqtt.strictness = "fail"` or `"require-sub"`, a rejected
publication similarly leaves the cursor unchanged. Publications earlier in the
same output group may therefore be replayed after the problem is corrected.

The multi-route schema is a breaking change: `journal.unit` moves to each
`[[route]]`, each route contains its own `publish_filter` and
`filter_strictness`, and `state_topic` belongs under `[mqtt]`. The former
`lokimankeli --cursor` startup override has been replaced by the one-shot
`start-position` state file.

## License

`lokimankeli` is free software licensed under the GNU General Public License,
version 3 or (at your option) any later version. See [`LICENSE`](LICENSE) for
the complete license text.
