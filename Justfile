# Tasks for this published mirror of the Sentinel Go Probe SDK (#160, ADR-0032).
#
# Run inside the Devbox shell from any directory: `devbox install` once, then
# `devbox shell` (or one-shot: `devbox run -- just test`). These recipes reuse the
# canonical gate names of Sentinel's source repository, scoped to this one language.
#
# The generated proto types under gen/ are committed here (a published mirror is
# consumable from a bare clone), so no recipe generates anything; regenerating is
# described in CONTRIBUTING.md.

build:
    GOTOOLCHAIN=local go build -mod=readonly ./...

test:
    GOTOOLCHAIN=local go test -mod=readonly -race ./...

lint:
    GOTOOLCHAIN=local go vet ./...

# `gofmt -l` exits 0 even when it lists unformatted files, so the `test -z` wrapper
# is what actually fails this gate (the same idiom as the source repository).
fmt-check:
    test -z "$(gofmt -l .)"
