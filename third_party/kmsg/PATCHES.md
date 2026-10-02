# Local patches to kmsg v1.13.1

This is `github.com/twmb/franz-go/pkg/kmsg` v1.13.1 with one change, wired in
by the `replace` directive in the repo's go.mod. Upstream's license applies
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

Note for embedders: a `replace` directive only applies to the main module being
built. A program that imports `github.com/doze-dev/doze-kafka/server` gets
upstream kmsg unless it carries the same `replace` (or upstream has fixed it).

To drop this fork: once upstream merges the equivalent fix, delete
third_party/kmsg and the replace line.
