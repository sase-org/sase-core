# sase_gateway

`sase_gateway` is the local workstation HTTP gateway for future SASE mobile clients. Phase 1 provides the Rust crate,
wire records, and route skeleton only; pairing, token storage, auth middleware, and SSE delivery are added by later
phases.

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

## Phase 1 Routes

- `GET /api/v1/health` returns an unauthenticated `HealthResponseWire`.
- `GET /api/v1/session` is registered as authenticated-route scaffolding and returns typed `unauthorized`.
- `GET /api/v1/events` is registered as authenticated-route scaffolding and returns typed `unauthorized`.
- Unknown routes return typed `not_found`.

No mutating route is registered in Phase 1.

## Local Run

```bash
cargo run -p sase_gateway -- --bind 127.0.0.1:0
```

The binary binds loopback by default. Bind-policy hardening and user-facing CLI integration live in later phases.
