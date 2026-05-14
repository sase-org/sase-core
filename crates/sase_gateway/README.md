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
- `GET /api/v1/session/push-subscriptions` lists active push subscriptions for the authenticated device.
- `POST /api/v1/session/push-subscriptions` registers or updates one hint-only push subscription for the authenticated
  device.
- `DELETE /api/v1/session/push-subscriptions/{id}` disables one push subscription for the authenticated device.
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
- `POST /api/v1/agents/launch` launches text agents through the fixed Python bridge, host-injects the authenticated
  device ID before dispatch, and preserves an optional `request_id` for client correlation.
- `POST /api/v1/agents/launch-image` validates and stores base64 image uploads under SASE-owned gateway state, then
  launches an agent whose prompt references the saved host path and preserves an optional `request_id`.
- `POST /api/v1/agents/{name}/kill` kills an exact agent name and persists mobile retry context.
- `POST /api/v1/agents/{name}/retry` retries an agent from durable mobile launch/kill context or artifact prompt data
  and preserves an optional `request_id` on the new launch context.
- `GET /api/v1/changespec-tags` lists active ChangeSpec workflow tags, optionally filtered by known project and limit.
- `GET /api/v1/xprompts/catalog` returns structured xprompt picker records, with optional best-effort PDF attachment
  metadata when `include_pdf=true`.
- `GET /api/v1/beads` lists open/in-progress beads by default, with known-project, cross-project, status/type/tier,
  closed, and limit filters.
- `GET /api/v1/beads/{id}` returns a structured bead detail record, including linked plan/design path display metadata
  when available.
- `POST /api/v1/update/start` starts the fixed SASE update worker and publishes `helpers_changed` on success.
- `GET /api/v1/update/{job_id}` reads structured update status and publishes `helpers_changed` when a status check
  observes the job.
- Unknown routes return typed `not_found`.

Device tokens are stored as SHA-256 hashes under `<sase_home>/mobile_gateway/devices.json`; raw bearer tokens are
returned only from the pairing finish response. Audit records are appended to `<sase_home>/mobile_gateway/audit.jsonl`
without secrets.

Mobile agent state is kept under `<sase_home>/mobile_gateway/`: launch contexts in `agent_launch_contexts.jsonl`, kill
contexts in `agent_kill_contexts/`, image uploads in `uploads/images/<device_id>/`, and last per-device project context
in `device_project_contexts/`. Project context IDs are product-shaped (`home`, `project:<name>`, or a persisted
`<project-context>:<workflow>:<ref>` VCS context) and are derived from known SASE projects, not arbitrary
client-supplied host paths.

Workflow helper routes call fixed `sase mobile helper-bridge <operation>` commands through the helper host bridge. They
do not accept mobile-supplied shell commands, cwd values, environment variables, project file paths, or arbitrary bridge
argv. ChangeSpec, xprompt, and bead helpers are read-only; the only mutating helper route starts the preconfigured
update worker.

`GET /api/v1/xprompts/catalog` preserves the Python helper bridge's xprompt editor metadata. Entries may include
`insertion`, `reference_prefix`, `kind`, and structured `inputs` records with `name`, `type`, `required`,
`default_display`, and `position`; clients should tolerate older helper output where those additive fields are absent.
The Rust gateway does not parse xprompt arguments itself.

The gateway currently reads notifications by polling the host JSONL store on each request. Successful notification state
and action mutations publish `notifications_changed` SSE events; passive file watching is intentionally left out of the
MVP.

Push delivery is disabled by default. When configured with `--push-provider test` or `--push-provider fcm`, event
emission attempts best-effort hint delivery to matching active subscriptions. Push hints contain safe IDs, categories,
reasons, and short display text only; clients must fetch authenticated gateway state after push receipt or notification
tap. Do not put bearer tokens, pairing codes, prompt bodies, response text, attachment contents, attachment tokens, host
paths, service-account contents, or signing material in provider payloads.

The cross-repo packaging, Tailscale Serve, troubleshooting, rollback, and threat-model runbook lives in the SASE repo:

```text
../sase_100/docs/mobile_mvp_runbook.md
```

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

Use workflow helper APIs for native mobile pickers and update status:

```bash
curl -sS "$BASE_URL/api/v1/changespec-tags?project=sase&limit=25" \
  -H "$AUTH_HEADER"

curl -sS "$BASE_URL/api/v1/xprompts/catalog?project=sase&tag=changespec&limit=50" \
  -H "$AUTH_HEADER"

curl -sS "$BASE_URL/api/v1/beads?project=sase&status=in_progress&limit=25" \
  -H "$AUTH_HEADER"

curl -sS "$BASE_URL/api/v1/beads/sase-26.4.6?project=sase" \
  -H "$AUTH_HEADER"

curl -sS -X POST "$BASE_URL/api/v1/update/start" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"schema_version":1,"request_id":"mobile-update-1"}'

curl -sS "$BASE_URL/api/v1/update/job_123" \
  -H "$AUTH_HEADER"
```

Every helper success response includes a common `result` object with `status`, `message`, `warnings`, `skipped`, and
`partial_failure_count`. Clients should use the structured status and skipped rows for control flow; `message` is
display text. Update status polling is authoritative in the MVP, even though update start/status checks also publish
`helpers_changed` events.

Duplicate, stale, ambiguous-prefix, already-handled, unsupported, and missing-target cases return typed `ApiErrorWire`
records and never overwrite existing response files.

## Local Run

```bash
cargo run -p sase_gateway -- --bind 127.0.0.1:0 --sase-home /tmp/sase
```

The binary binds `127.0.0.1:7629` by default. Non-loopback binds such as `0.0.0.0:7629`, LAN addresses, or tailnet
addresses fail unless `--allow-non-loopback` / `-L` is passed explicitly.

Push-provider examples:

```bash
cargo run -p sase_gateway -- --push-provider test
cargo run -p sase_gateway -- \
  --push-provider fcm \
  --fcm-project-id my-firebase-project \
  --fcm-credential-env SASE_FCM_CREDENTIAL \
  --fcm-dry-run
```

## Local Daemon Shadow Indexing

The same binary also runs the host-local daemon used by `sase daemon start`. Daemon mode owns a Unix-socket framed JSON
RPC surface, projection storage, shadow indexing, and watcher health diagnostics. It does not make daemon projections
authoritative for CLI, ACE, editor, mobile, or workflow reads yet: current source stores remain the source of truth
until a later indexed-read epic routes those callers deliberately.

Supported shadow-index surfaces are:

- `changespecs` for active and archive project spec files.
- `notifications` for notification and pending-action stores.
- `agents` for agent artifact directories, marker files, archives, and dismissed state.
- `beads` for project bead stores and dependency/ready/query projections.
- `catalogs` for xprompt/workflow/config/memory/file-history style catalogs.
- `all` for a combined operation over every supported surface.

Useful operator commands from the Python checkout:

```bash
sase daemon start
sase daemon doctor --json
sase daemon rebuild --surface all --json
sase daemon verify --surface all
sase daemon diff --surface all --limit 100 --json
```

`sase daemon rebuild` performs source-store backfill through the live daemon. Pass
`--surface changespecs|notifications|agents|beads|catalogs` to narrow the operation, and `--project <id>` for
project-scoped surfaces. The `--reset-storage` flag keeps the lower-level projection-table reset/replay recovery path;
this is the only rebuild mode available when the daemon is stopped.

`sase daemon verify` returns compact per-surface diff counts and exits nonzero when any projected row disagrees with the
current loaders. `sase daemon diff` returns bounded diff records categorized as `missing`, `stale`, `extra`, or
`corrupt`, with source paths and handles where available. These commands are diagnostic surfaces only; an empty diff
means the shadow index agrees with the current loaders, not that production reads are using the daemon.

### Shadow Indexing Playbook

- Indexing degraded: run `sase daemon doctor --json` and check `details.indexing`. If `watcher_active` is false or
  dropped/coalesced counters keep rising, restart the daemon and then run a scoped rebuild for the affected surface.
- Projection/source mismatch: run `sase daemon diff --surface <surface> --json` and inspect the diff categories. Use
  scoped rebuild first; only use `--reset-storage` when the projection database itself is suspect.
- Stale watcher roots: confirm the source path exists under the expected SASE home or project root, then run
  `sase daemon rebuild --surface <surface>`. Reconciliation should repair missed watcher events, but missing roots
  require the daemon to be started with the correct `--sase-home` and project context.
- Corrupt projection database: stop the daemon, run `sase daemon rebuild --reset-storage --json`, start the daemon
  again, then run `sase daemon verify --surface all`.
- Large rebuild in progress: prefer `sase daemon doctor --json` over repeated diffs. The doctor payload reports queue
  counters and recent reports without requesting large diff pages.

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

The committed local daemon contract scaffold lives separately at:

```text
crates/sase_gateway/contracts/local_daemon/v1/local_daemon_v1.json
```

It records the Unix-socket framed-JSON envelope, schema compatibility policy, health/capabilities/list/event shapes,
shadow-index diagnostics, paging/cursor/snapshot bounds, and explicit no-daemon fallback signaling. Regenerate it after
intentional local daemon wire-shape changes with:

```bash
cargo run -p sase_gateway -- --local-daemon-contract-out crates/sase_gateway/contracts/local_daemon/v1/local_daemon_v1.json
```

Epic 4 handoff: local daemon transport, startup, shadow source-store indexing, rebuild/verify/diff diagnostics, and
contract snapshots are available. Production read routing remains intentionally deferred to the indexed-read epic.
Validation: `cargo test -p sase_gateway` and focused projection tests in `../sase-core`, plus `just install` and
`just check` in the Python checkout after Python-side changes.

The SASE checkout's Phase 1E readiness review links this local daemon snapshot to the fixture, perf, and compatibility
matrix artifacts at `sdd/research/202605/rust_daemon_epic1_readiness.md`.

MVP limitations: notification reads are polling-backed REST reads; only gateway mutations publish state-change SSE
events; oversized or path-unsafe attachments are listed without tokens; agent project context selects only known SASE
projects or prompt-declared VCS refs; helper routes are read-only except update start; xprompt PDF generation is
optional; update status polling is authoritative; and legacy Telegram pending-action JSON remains a compatibility source
while Telegram adoption of the shared store is incremental.
