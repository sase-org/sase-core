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
- `GET /api/v1/events` requires auth and streams `EventRecordWire` records over SSE. Heartbeat events use monotonic IDs,
  and reconnects may pass `Last-Event-ID` to replay buffered newer events. The first implementation keeps an in-memory
  ring buffer, so clients should handle `resync_required` after a restart or buffer overflow by fetching full state.
- `GET /api/v1/notifications` requires auth and returns newest-first mobile notification cards from
  `<sase_home>/notifications/notifications.jsonl`. Supported query fields are `unread`/`unread_only`,
  `include_dismissed`, `include_silent`, `limit`, and `newer_than`.
- `GET /api/v1/notifications/{id}` requires auth and returns one notification with full notes, action detail, and
  attachment manifests. Detail responses mint short-lived attachment tokens for regular declared files that pass path
  and size checks; list cards only expose counts and kinds.
- `POST /api/v1/notifications/{id}/mark-read` requires auth, marks one notification read, returns the resulting state,
  audits the attempt, and publishes `notifications_changed` on success.
- `POST /api/v1/notifications/{id}/dismiss` requires auth, marks one notification dismissed, returns the resulting
  state, audits the attempt, and publishes `notifications_changed` on success.
- `GET /api/v1/attachments/{token}` requires auth and streams the declared attachment bytes. Tokens are bound to the
  device that inspected the detail response, expire after a short TTL, are never stored in the audit log, and are
  rejected if the target path disappears, changes size, crosses a symlink, contains traversal, is not a regular file, or
  exceeds the MVP download limit.
- `POST /api/v1/actions/plan/{prefix}/approve|run|reject|epic|legend|feedback` writes `plan_response.json` for a
  currently pending plan notification.
- `POST /api/v1/actions/hitl/{prefix}/accept|reject|feedback` writes `hitl_response.json` for a pending HITL
  notification.
- `POST /api/v1/actions/question/{prefix}/answer|custom` validates `question_request.json` and writes
  `question_response.json` for a pending user-question notification.
- `GET /api/v1/agents` lists running agents by default, with optional recent/status/project/limit filters.
- `GET /api/v1/agents/resume-options` returns native copy/share/direct-launch resume and wait prompt options.
- `POST /api/v1/agents/launch` launches text agents through the fixed Python bridge and host-injects the authenticated
  device ID before dispatch.
- `POST /api/v1/agents/launch-image` validates and stores base64 image uploads under SASE-owned gateway state, then
  launches an agent whose prompt references the saved host path.
- `POST /api/v1/agents/{name}/kill` kills an exact agent name and persists mobile retry context.
- `POST /api/v1/agents/{name}/retry` retries an agent from durable mobile launch/kill context or artifact prompt data.
- Unknown routes return typed `not_found`.

Device tokens are stored as SHA-256 hashes under `<sase_home>/mobile_gateway/devices.json`; raw bearer tokens are
returned only from the pairing finish response. Audit records are appended to `<sase_home>/mobile_gateway/audit.jsonl`
without secrets.

Mobile agent state is kept under `<sase_home>/mobile_gateway/`: launch contexts in `agent_launch_contexts.jsonl`, kill
contexts in `agent_kill_contexts/`, image uploads in `uploads/images/<device_id>/`, and last per-device project context
in `device_project_contexts/`. Project context IDs are product-shaped (`home`, `project:<name>`, or a persisted
`<project-context>:<workflow>:<ref>` VCS context) and are derived from known SASE projects, not arbitrary
client-supplied host paths.

The gateway currently reads notifications by polling the host JSONL store on each request. Successful notification state
and action mutations publish `notifications_changed` SSE events; passive file watching is intentionally left out of the
MVP.

## Curl Examples

After pairing, set the bearer token returned by `POST /api/v1/session/pair/finish`:

```bash
BASE_URL="http://127.0.0.1:7629"
TOKEN="sase_mobile_example"
AUTH_HEADER="Authorization: Bearer $TOKEN"
```

List unread notifications:

```bash
curl -sS "$BASE_URL/api/v1/notifications?unread=true&limit=25" \
  -H "$AUTH_HEADER"
```

Inspect one plan approval notification and mint attachment tokens for its detail view:

```bash
NOTIFICATION_ID="abcdef12-plan"

curl -sS "$BASE_URL/api/v1/notifications/$NOTIFICATION_ID" \
  -H "$AUTH_HEADER"
```

Mark one notification read or dismissed:

```bash
curl -sS -X POST "$BASE_URL/api/v1/notifications/$NOTIFICATION_ID/mark-read" \
  -H "$AUTH_HEADER"

curl -sS -X POST "$BASE_URL/api/v1/notifications/$NOTIFICATION_ID/dismiss" \
  -H "$AUTH_HEADER"
```

Plan actions accept either the full notification ID or a unique pending-action prefix:

```bash
PREFIX="abcdef12"

curl -sS -X POST "$BASE_URL/api/v1/actions/plan/$PREFIX/approve" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"commit_plan":true,"run_coder":false}'

curl -sS -X POST "$BASE_URL/api/v1/actions/plan/$PREFIX/run" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"coder_prompt":"Focus on tests"}'

curl -sS -X POST "$BASE_URL/api/v1/actions/plan/$PREFIX/reject" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"feedback":"Please narrow the scope"}'

curl -sS -X POST "$BASE_URL/api/v1/actions/plan/$PREFIX/feedback" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"feedback":"Revise the rollout section"}'
```

HITL and question actions write the same response-file shapes as existing TUI and Telegram flows:

```bash
curl -sS -X POST "$BASE_URL/api/v1/actions/hitl/hitl0001/accept" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1}'

curl -sS -X POST "$BASE_URL/api/v1/actions/hitl/hitl0002/feedback" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"feedback":"Use a smaller change"}'

curl -sS -X POST "$BASE_URL/api/v1/actions/question/quest001/answer" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"selected_option_id":"safe","global_note":"Use durable path"}'

curl -sS -X POST "$BASE_URL/api/v1/actions/question/quest002/custom" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"custom_answer":"Use SQLite"}'
```

Download an attachment with a token from a detail response:

```bash
ATTACHMENT_TOKEN="att_example"

curl -sS "$BASE_URL/api/v1/attachments/$ATTACHMENT_TOKEN" \
  -H "$AUTH_HEADER" \
  -o attachment.bin
```

List, launch, kill, and retry agents:

```bash
curl -sS "$BASE_URL/api/v1/agents?include_recent=true&limit=25" \
  -H "$AUTH_HEADER"

curl -sS "$BASE_URL/api/v1/agents/resume-options" \
  -H "$AUTH_HEADER"

curl -sS -X POST "$BASE_URL/api/v1/agents/launch" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"project":"sase","prompt":"Run the focused tests","name":"mobile.tests"}'

IMAGE_B64="$(base64 -w0 screenshot.png)"
IMAGE_BYTES="$(wc -c < screenshot.png)"
curl -sS -X POST "$BASE_URL/api/v1/agents/launch-image" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d "{\"schema_version\":1,\"prompt\":\"Review this screenshot\",\"name\":\"mobile.image\",\"original_filename\":\"screenshot.png\",\"content_type\":\"image/png\",\"byte_length\":$IMAGE_BYTES,\"base64_image\":\"$IMAGE_B64\"}"

curl -sS -X POST "$BASE_URL/api/v1/agents/mobile.tests/kill" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"reason":"mobile"}'

curl -sS -X POST "$BASE_URL/api/v1/agents/mobile.tests/retry" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1}'
```

Duplicate, stale, ambiguous-prefix, already-handled, unsupported, and missing-target cases return typed `ApiErrorWire`
records and never overwrite existing response files.

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

MVP limitations: notification reads are polling-backed REST reads; only gateway mutations publish state-change SSE
events; oversized or path-unsafe attachments are listed without tokens; agent project context selects only known SASE
projects or prompt-declared VCS refs; and legacy Telegram pending-action JSON remains a compatibility source while
Telegram adoption of the shared store is incremental.
