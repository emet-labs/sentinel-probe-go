# Contributing to Go Probe SDK

This repository is a **published mirror**. Its source of truth is Emet Labs' internal
Sentinel monorepo, and this tree is produced by an automated publish pipeline.

- **Issues** are real, public-facing issues: bug reports and feature requests filed here
  are triaged and worked in the source repository, and the fix arrives here on the next
  publish.
- **Pull requests** are welcome and reviewed. Because this tree is generated, an accepted
  change is re-landed in the source repository and flows back here through the next
  publish. Please sign off every commit (`Signed-off-by: Your Name <you@example.com>`,
  DCO) so the re-landing is unambiguous.
- **Licence**: MPL-2.0 (see [LICENSE](LICENSE)). Contributions are accepted under the
  same licence.

## Development

Set up the pinned environment with `devbox install`, then run tasks with `just` (see the
Development environment section of the README). A few code comments reference paths from
the source repository this mirror is published from; read those as provenance.

## Regenerating the proto types

`proto/` and the pinned buf template at this repository's root carry everything the
codegen needs; the generated tree is committed so a bare clone builds without buf. After
editing `proto/`, regenerate with the pinned buf version using this repository's own
template, and commit the result.
