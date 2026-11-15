# Probe SDK conformance fixtures

This directory is the language-neutral test contract for handwritten Probe behavior. The
protobuf files and ADR-0023/0024 are authoritative; these fixtures record examples of that
contract and do not replace the wire schema. TypeScript is consulted only where those
authorities are silent.

`manifest-v1.json` is the only discovery entry point. Version 1.0.0 is the previous
version: it accepts `spec_match`, `int128`, and `enforcement_gate`, rejects unknown fields
and tokens, and rejects duplicate JSON object keys at every depth. All integers are
canonical base-10 strings. Adding a field, suite, or operation requires a new format
version and coordinated updates to every SDK loader.

Version 1.1.0 registers the `otlp_signal` suite. Its cases are the golden requests in
`testdata/otlp/`, named by path, so a loader cannot skip the wire. Protobuf bytes stay in
those files. A case marked `reject` must fail in every loader. The checker still accepts
version 1.0.0 documents as the previous version and rejects a loader that does not
implement `otlp_signal`.

Version 1.3.0 registers the `sequence_completeness` suite (#461): the Probe-side Sequence
allocator and completeness heartbeat every SDK exports. Suite-document versions are claimed
per suite kind in advance. The manifest version is assigned at merge: the greater of the
previous manifest version plus one minor and the highest suite version listed. The manifest
version can therefore skip a version that a suite document claims.

Version 1.2.0 holds the `decision_wait` and `filter_store` suites (#464), and manifest 1.4.0
lists them. Every SDK runs both through its production code: the log receiver and its
decision adapter, and the FilterStore. Only the timer or the clock is injected.

`sequence-completeness-v1.json` holds two case shapes, told apart by their keys, with ids
unique across both:

- A **scenario** case (`id`, `start`, `steps`) drives one allocator and one heartbeat
  against an injected clock (`clock_unix_ms`; nanoseconds are milliseconds × 10^6), an
  injected Filter-epoch reader and an injected send function. The step `op`s are
  `allocate`, `export_ok`, `export_fail`, `filter_drop`, `set_filter_epoch`, `clock`,
  `idle`, `active`, `tick` (`report()`), `shutdown` and `restart` (a new allocator and
  heartbeat; `previous_epoch: "inherit"` passes the old allocator's epoch, and on
  `expect_error` the old pair stays in use). `tick` and `shutdown` list one `send` outcome
  per signal sent, the expected signals in order, and the expected filtered ranges. For
  every expected signal a loader asserts every listed field, the presence of
  `last_emitted`, `complete_through` and `acknowledged_filter_epoch` (including zero), the
  clock domain `unix`, an uncertainty of 0, and `restates_previous`. `restates_previous`
  is true exactly when the signal, re-encoded with the previous sent signal's
  `producer_reported_at` nanoseconds, is byte-identical to that previous signal's
  encoding. `expect_error` names a stable error code: `epoch-not-greater`,
  `sequence-overflow` or `filter-epoch-regressed`.
- A **partial-success** case (`id`, `response_hex`, and `rejected_spans` or
  `undecodable: true`) pins how every SDK reads an `ExportTraceServiceResponse` body. An
  export succeeded only on HTTP 2xx with `rejected_spans` 0 (absent counts as 0); an
  undecodable 2xx body is a failure.

`decision-wait-v1.json` pins how long a Probe waits for a `DecideResponse` log. Each case
names its `request_id` and an `answer` golden (`../otlp/decide-response.bin`, which answers
`req-charge-1` with Deny). `before_ask: ["deliver"]` delivers the answer to the receiver
before the ask. `on_wait` lists what happens once the wait is armed: `deliver` delivers the
answer, then `expire` fires the expiry. `expected.armed_wait_ns` is the wait the receiver
arms, or null when it arms none. There are two case shapes:

- `via: "gate"` runs the production gate with the receiver's adapter as its decision
  dependency. The fields are the `enforcement_gate` fields, and the gate reads its clock
  exactly twice. `expected` holds the outcome `kind`, `decide_calls` and the request's
  `remaining_transport_budget_ns`, which is also the armed wait. No `reason` is pinned.
- `via: "adapter"` calls the adapter directly with `remaining_transport_budget_ns` (null
  is absent). `expected.result` is the answer's action or `error`. An absent or zero budget
  arms no wait and is an error unless the answer was already delivered.

A runner asserts the outcome, that the wait was armed exactly once for `armed_wait_ns` or
never, and, for a gate case, `decide_calls`, the request's budget and that the gate's clock
script was fully consumed. Each SDK maps the steps onto its own seam:

| SDK | `deliver` | `expire` | Armed |
|---|---|---|---|
| TypeScript | POST to the receiver from the `decisionTimer` | call `expire()` | `decisionTimer` called once with `armed_wait_ns` |
| Go | `ServeHTTP` in-process from the `DecisionTimer` | return an already-fired channel | `DecisionTimer` called once with `armed_wait_ns` |
| Python | `accept()` on the first receiver clock read | the clock reads `anchor + armed - 1`, then `anchor + armed` | the first read is the arm; the wait expires at `anchor + armed` and not before. The `expire` reads are consumed only when no answer was delivered |
| JVM | POST to `logUrl` from `DecisionTimer.arm` | call `onExpire()` | `arm` called once with `armed_wait_ns` |
| .NET | `Ingest` from the `TimeProvider`'s `CreateTimer` | fire the timer callback | one timer created with a due time of `armed_wait_ns` |
| Elixir | POST to the receiver origin from `decision_timer:` | send `{:decision_timeout, ref}` | `decision_timer:` called once with `armed_wait_ns` in milliseconds |

Every budget in a `decision_wait` case is a whole number of milliseconds, at least 1: below
1 ms the SDKs still differ (#536).

`filter-store-v1.json` pins the Filter-epoch order. A case seeds the store with `initial`
(or nothing) and runs `set` and `should_refresh` steps. Each Filter is `{label, epoch}`: the
label is its single `specification_id`, and the epoch is a decimal string below 2^64 or null
(absent). A `set` step asserts the returned `applied`, the held Filter's label and the
held epoch's presence and value, and that the held Filter is the one last applied (Elixir
compares the term). A `set` at an epoch equal to the held one lists no `applied`: it pins
only the held label and epoch, so the case holds whatever #534 decides, and a runner stops
asserting the held reference for the rest of that case. A `should_refresh` step asserts the
returned boolean.

Beyond the schema, the checker recomputes these two suites. For `decision_wait` it
recomputes the gate's remaining budget and requires `armed_wait_ns` to equal it, requires
`on_wait` to be empty exactly when no wait is armed, and derives each expected outcome from
the answer golden, the request id and the fail mode. For `filter_store` it replays the epoch
order over every step, and it refuses an equal epoch with a different label, so #534's open
case cannot be written into a vector. `tools/test-probe-sdk-conformance-checker.py` breaks
committed vectors one at a time and asserts the checker refuses each.

Beyond the schema, the checker proves that the vectors pin the never-ahead invariant
without any SDK. Each expected `complete_through` must equal the contiguous run, from 1,
of slots that earlier `export_ok` steps named for that epoch. A slot dropped by the Event
Filter does not count until #542. Each `last_emitted` must equal the highest number an
earlier `allocate` returned for that epoch. `restates_previous` must agree with the listed
fields. In `sequence_completeness`, Sequence epochs, sequence numbers and Filter epochs stay
at or below 2^53 − 1, so the TypeScript loader can execute every vector.

The files are handwritten and reviewed. Generated protobuf output must never be copied here.
Absolute `local_deadline_ns` values are process-local monotonic values; only relative
`remaining_transport_budget_ns` values may enter a DecideRequest.
