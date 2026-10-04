# Local patches to kmsg v1.13.1

This is `github.com/twmb/franz-go/pkg/kmsg` v1.13.1 with one change, kept inside
the module as `internal/kmsg` and imported by the broker's own code. Upstream's license applies
unchanged (see LICENSE).

## internalReadTags: stop when the reader has failed (api.go)

`internalReadTags` loops `for num := b.Uvarint(); num > 0; num--` and never
checks whether the reader ran out of bytes. `num` is read straight off the wire
(up to 2^32-1), so a ~70-byte flexible request whose tagged-field count is
larger than its remaining bytes keeps iterating — three failed varint reads and
a map write each — for well over a minute after the data is gone. Every
flexible request type reads tags through this function, so any such API is a
single-request CPU denial of service.

Found by `FuzzParseAndDecodeRequest` (the weekly fuzz job's "fuzzing process
hung or terminated unexpectedly"). Reproduced on v1.13.1 and v1.14.0; not fixed
upstream as of 2026-10. The patch adds `&& b.Ok()` to the loop condition.

`internal/protocol/codec_test.go` fails if this copy is not the one in use.

It is a package of this module rather than a `replace` directive on purpose. A
`replace` only applies to the main module being built: anyone importing
`github.com/doze-dev/doze-kafka/server` would have compiled the broker against
upstream kmsg and had the hang back, and `go install …@version` refuses modules
that carry one. `TestBrokerCodeUsesTheInModuleKmsg` keeps both from returning.

The console and the tests still use upstream kmsg through franz-go's client; the
two are wire-compatible and never exchange Go values.

To drop this fork: once upstream merges the equivalent fix, delete internal/kmsg and
point the imports back at `github.com/twmb/franz-go/pkg/kmsg`.
