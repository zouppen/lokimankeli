# journal-mqtt

`journal-mqtt` follows the stdout records of one systemd service, decodes each
`MESSAGE` as a JSON object, transforms it with jq, and publishes it to MQTT.
It reads journald directly and does not invoke `journalctl` or `stdbuf`.

## Message flow

Two required jq programs control routing and content:

1. `topic_filter` receives the original JSON object and must emit exactly one
   MQTT topic string.
2. `content_filter` then receives the same original object. It may emit zero,
   one, or several JSON values. Each value becomes a separate MQTT message on
   the selected topic.

The content filter also receives `$timestamp`, the journal receive time in Unix
milliseconds, and `$event_id`, an unpadded base64url HMAC-SHA256 of the journal
cursor. Multiple outputs from one journal record share an event ID.

```jq
del(.private)
| .observed_at = $timestamp
| .source_event = $event_id
```

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
journal-mqtt --config /path/to/bridge.toml --cursor 's=...;i=...;b=...;m=...;t=...;x=...'
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
instance. For the system unit, create the unprivileged `journal-mqtt` account
and grant it membership in `systemd-journal`. A user unit reads only the current
user journal and should set `journal.scope = "user"`.

The MQTT identity needs permission to publish telemetry and read/write its
configured state topic. Telemetry-only consumers should be denied access to
the state topic. Enable TLS whenever the broker connection crosses an
untrusted network.

## Failure semantics

The cursor advances only after all derived MQTT publications are acknowledged.
A crash between telemetry acknowledgement and checkpoint acknowledgement may
replay the complete output group; `$event_id` remains stable. Invalid JSON,
unroutable records, jq runtime failures, and an intentional zero-output content
filter are skipped and checkpointed so one poison record cannot block the
stream.
