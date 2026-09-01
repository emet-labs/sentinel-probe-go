# Probe SDK conformance fixtures

This directory is the language-neutral test contract for handwritten Probe behavior. The
protobuf files and ADR-0023/0024 are authoritative; these fixtures record examples of that
contract and do not replace the wire schema. TypeScript is consulted only where those
authorities are silent.

`manifest-v1.json` is the only discovery entry point. Version 1 accepts exactly `1.0.0`,
rejects unknown fields and tokens, and rejects duplicate JSON object keys at every depth.
All integers are canonical base-10 strings. Adding a field, suite, or operation requires a
new format version and coordinated updates to every SDK loader.

The files are handwritten and reviewed. Generated protobuf output must never be copied here.
Absolute `local_deadline_ns` values are process-local monotonic values; only relative
`remaining_transport_budget_ns` values may enter a DecideRequest.
