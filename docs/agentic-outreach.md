# Agentic Outreach Boundary

OpenFang owns browser execution. Studio OS owns company state.

## Boundary

- OpenFang owns platform manifests, browser profiles, login/session lifecycle,
  source capture, and dispatch execution.
- Studio OS owns candidate leads, pursuit runs, source capture records,
  outreach drafts, send attempts, review decisions, authorizations, and audit
  events.
- Agents may interpret captured source text and draft messages. They must not
  directly control browser clicks for paid or external side effects.

## Flow

1. Studio OS creates or updates a `pursuit_run`.
2. OpenFang selects an outreach platform manifest from the structured source
   field, not from page text or host guessing.
3. OpenFang captures the source page using the platform profile and allowlist.
4. OpenFang writes the captured page to Studio OS via `POST /api/source_captures`
   and updates the pursuit session result.
5. The Studio OS continuation asks `studio-lead` to extract requirements and
   create an `outreach_draft`.
6. The operator reviews, edits, rewrites, approves, or cancels in Studio OS.
7. Studio OS creates a `send_attempt` only after an approved draft exists.
8. OpenFang dispatches only when Studio OS has an approved draft, quoted cost,
   one-time authorization, configured selector environment values, an
   allowlisted URL, and an attempt record.
9. OpenFang writes terminal or blocked results back to Studio OS.

## Failure Rules

- Missing platform manifest: block.
- Missing profile/session: block and ask Studio OS to surface login.
- Missing source capture: do not ask an agent to infer from memory.
- Missing send cost for a paid platform: block authorization and dispatch.
- Missing selector config: block. Do not fall back to visible button text.
- Redirect outside the manifest allowlist: block.
- Unknown confirmation dialog after click: block, record the state, and let the
  operator decide.

## Platform Manifests

Outreach platform manifests live under `manifests/outreach/*.toml`. New
platforms should start there. Core Studio OS code should not gain source-specific
branches for new platforms.

Selector fields point to required environment variable names. The runtime must
resolve those variables before dispatch and fail closed when a selector is not
configured.
