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
3. Studio OS invokes `openfang outreach inspect` for the selected manifest.
4. OpenFang captures the source page using the platform profile and allowlist,
   then returns the raw capture payload for Studio OS to persist as a durable
   `source_captures` row and pursuit session result.
5. The Studio OS continuation asks `studio-lead` to extract requirements and
   create an `outreach_draft`.
6. The operator reviews, edits, rewrites, approves, or cancels in Studio OS.
7. Studio OS creates a `send_attempt` only after an approved draft exists.
8. Studio OS invokes `openfang outreach dispatch` only when it has an approved
   draft, quoted cost, one-time authorization, configured selector environment
   values, an allowlisted URL, and an attempt record.
9. OpenFang returns terminal or blocked results for Studio OS to record.

## Failure Rules

- Missing platform manifest: block.
- Missing profile/session: block and ask Studio OS to surface login.
- OpenFang runner unavailable: block; Studio OS has no browser fallback for
  outreach execution.
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

## Runner Commands

`openfang outreach inspect` captures an allowlisted source page.
`openfang outreach open-login` opens the platform login profile.
`openfang outreach login-inspect` opens login, waits for an observed
authenticated URL, then captures the source page.
`openfang outreach verify-selectors` checks explicitly configured selectors.
`openfang outreach dispatch` fills the configured message selector and clicks
the configured send selector only after the caller has already created and
authorized the attempt. Paid manifests must pass `--expected-cost-label`; the
runner observes the current page before filling or clicking, emits a structured
`cost_snapshot` from the matched visible text, and blocks when that cost label
is not visible.
Dispatch also blocks when the configured success selector is already visible
before click, because that selector cannot prove a new send occurred. Successful
dispatches write a 24-hour idempotency record keyed by platform, source URL, and
message body so accidental repeated calls return the cached sent result instead
of sending the same message twice.

Profile access uses an OS-backed non-blocking lock plus metadata, so stale lock
files left by a killed process do not block future runs. `open-login` writes a
login browser PID record and reuses a live login window instead of spawning
unbounded browser processes.
