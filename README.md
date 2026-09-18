# lokimankeli

`lokimankeli` follows the stdout records of one systemd service, decodes each
`MESSAGE` as a JSON object, transforms it with jq, and publishes it to MQTT.
It reads journald directly and does not invoke `journalctl` or `stdbuf`.

## Message flow

The required `publish_filter` jq program receives the original JSON object and
emits zero or more MQTT publication descriptors. Each result must contain
exactly a `topic` string and a `payload` JSON value. This lets one journal entry
produce different payloads on different topics.

The filter also receives `$timestamp_ms`, the journal receive time in Unix
milliseconds, and `$event_id`, an unpadded base64url HMAC-SHA256 of the journal
cursor. Multiple publications from one journal record share an event ID.
The older `$timestamp` variable is not defined; filters must use
`$timestamp_ms` so the unit is explicit.

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

## Install

Python 3.11 or newer is required. On Debian, the native dependencies are
available as `python3-systemd`, `python3-paho-mqtt`, and `python3-jq`. When
installing dependencies from PyPI, building `systemd-python` may additionally
require `libsystemd-dev`, `pkg-config`, a compiler, and Python development
headers.

Install the project with your normal Python packaging workflow, then copy
[`examples/config.toml`](examples/config.toml) to a location of your choice and
restrict its permissions. The program deliberately has no default config path:

```console
lokimankeli --config /path/to/bridge.toml --cursor 's=...;i=...;b=...;m=...;t=...;x=...'
```

`--cursor` is required for the first run because no retained checkpoint exists
yet. It identifies the last already-handled record; processing begins with the
next matching record. Obtain a suitable `__CURSOR` from `journalctl -o json`.
The override is validated and saved before delivery begins. Later starts omit
`--cursor` and resume from retained MQTT state. The bridge refuses to guess a
position if the retained state is missing or invalid.

The config file contains the MQTT password and HMAC key. Use mode `0640` with a
dedicated service group for a system service, or `0600` for a user service.

## Service deployment

Example system and user units are under [`systemd/`](systemd/). They contain
illustrative explicit config paths; edit `ExecStart` to the path chosen for each
instance. For the system unit, create the unprivileged `lokimankeli` account
and grant it membership in `systemd-journal`. A user unit reads only the current
user journal and should set `journal.scope = "user"`.

The MQTT identity needs permission to publish telemetry and read/write its
configured state topic. Telemetry-only consumers should be denied access to
the state topic. Enable TLS whenever the broker connection crosses an
untrusted network.

## Failure semantics

The cursor advances only after all derived MQTT publications are acknowledged.
A crash between telemetry acknowledgement and checkpoint acknowledgement may
replay the complete output group; `$event_id` remains stable. Invalid JSON, jq
runtime failures, invalid publication descriptors, and an intentional
zero-output filter are skipped and checkpointed so one poison record cannot
block the stream.

Configurations from the initial two-filter design must replace
`topic_filter` and `content_filter` with `publish_filter`. The program reports a
specific migration error if either legacy key is present.

## License

`lokimankeli` is free software licensed under the GNU General Public License,
version 3 or (at your option) any later version. See [`LICENSE`](LICENSE) for
the complete license text.
