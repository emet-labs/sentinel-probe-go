// Package gensmoke exists only to host the proto codegen smoke test.
//
// It has no production surface. Its job is to fail loudly and early if the generated tree in
// sdk/go/gen/ stops matching what the hand-written packages assume: import paths, package
// clauses, the proto-prefixed enum constant names protoc-gen-go emits, the Connect client and
// handler constructors, and the `optional` presence semantics the enforcement gate depends on.
//
// It lives under internal/ because none of that is public SDK API, and it carries a doc.go so
// the directory holds a buildable package rather than test files alone.
package gensmoke
