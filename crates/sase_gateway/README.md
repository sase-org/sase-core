# sase_gateway

`sase_gateway` is the local workstation HTTP gateway for future SASE mobile clients. It provides health, pairing,
authenticated session routes, and an authenticated SSE event stream.

## Response Shape

Successful responses are direct JSON records with `schema_version` as the first field. They are not wrapped in a common
envelope. Optional fields serialize as explicit JSON `null`, and empty lists serialize as `[]`.

Errors use a single `ApiErrorWire` record:

```json
{
  "schema_version": 1,
  "code": "unauthorized",
  "message": "authentication is required for this endpoint",
  "target": "authorization",
  "details": null
}
```

The HTTP status code carries transport status, while `code` is the stable client-facing error identifier.

## Routes

- `GET /api/v1/health` returns an unauthenticated `HealthResponseWire`.
- `POST /api/v1/session/pair/start` returns a short-lived one-time pairing code and no long-lived credential.
- `POST /api/v1/session/pair/finish` exchanges the one-time code and device metadata for a bearer token exactly once.
- `GET /api/v1/session` requires `Authorization: Bearer <token>` and returns the authenticated device.
- `GET /api/v1/events` requires auth and streams `EventRecordWire` records over SSE. Heartbeat events use monotonic
  IDs, and reconnects may pass `Last-Event-ID` to replay buffered newer events. The first implementation keeps an
  in-memory ring buffer, so clients should handle `resync_required` after a restart or buffer overflow by fetching full
  state.
- `GET /api/v1/notifications` requires auth and returns newest-first mobile notification cards from
  `<sase_home>/notifications/notifications.jsonl`. Supported query fields are `unread`/`unread_only`,
  `include_dismissed`, `include_silent`, `limit`, and `newer_than`.
- `GET /api/v1/notifications/{id}` requires auth and returns one notification with full notes, action detail, and
  attachment manifest placeholders. Phase 2 manifests include display metadata and no download tokens yet.
- Unknown routes return typed `not_found`.

Device tokens are stored as SHA-256 hashes under `<sase_home>/mobile_gateway/devices.json`; raw bearer tokens are
returned only from the pairing finish response. Audit records are appended to `<sase_home>/mobile_gateway/audit.jsonl`
without secrets.

The gateway currently reads notifications by polling the host JSONL store on each request. Mutating notification routes
in later phases should publish `notifications_changed` SSE events; passive file watching is intentionally left out of
Phase 2.

## Local Run

```bash
cargo run -p sase_gateway -- --bind 127.0.0.1:0 --sase-home /tmp/sase
```

The binary binds `127.0.0.1:7629` by default. Non-loopback binds such as `0.0.0.0:7629`, LAN addresses, or tailnet
addresses fail unless `--allow-non-loopback` / `-L` is passed explicitly.

## Contract Snapshot

The committed mobile API contract snapshot lives at:

```text
crates/sase_gateway/contracts/api_v1/mobile_api_v1.json
```

It records the API base path, auth scheme, route list, wire-record field names, event resume header, and example pairing
payloads for future mobile/client phases. Regenerate it after route or wire-shape changes with:

```bash
cargo run -p sase_gateway -- --contract-out crates/sase_gateway/contracts/api_v1/mobile_api_v1.json
```
