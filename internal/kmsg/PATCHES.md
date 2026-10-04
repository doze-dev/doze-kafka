# Local patches to kmsg v1.13.1

This is `github.com/twmb/franz-go/pkg/kmsg` v1.13.1 with three changes, kept inside
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

## Arrays grow as they are decoded (generated.go, 424 sites)

Every array was decoded as `make([]T, count)` followed by a loop, where `count`
comes off the wire and is only checked against the number of bytes left (one byte
per element). `T` is often an 80-byte struct, so a 4 MiB request whose array count
claimed 4 million elements made the decoder allocate 344 MiB before discovering
there was nothing to read: about 86 times the request size, and with 100 MiB
frames, gigabytes from one request.

The loop now appends one element at a time and stops when the reader has failed,
so memory follows the elements that are really present. The change is mechanical
(one regular expression over the uniform generated pattern):

```go
// before                                   // after
if l > 0 {                                  for i := int32(0); i < l && b.Ok(); i++ {
    a = append(a, make([]T, l)...)              a = append(a, make([]T, 1)...)
}                                               v := &a[i]
for i := int32(0); i < l; i++ {
    v := &a[i]
```

`internal/protocol/codec_test.go` (`TestDecodeDoesNotAllocateWhatTheCountClaims`)
fails if a regenerated file brings the old pattern back.

## A ceiling on array counts (internal/kbin/primitives.go)

`ArrayLen`, `CompactArrayLen` and `VarintArrayLen` refuse a count above
`MaxArrayLen` (2^20). Lazy growth bounds memory by the elements really present,
but a large frame of validly encoded empty elements is still millions of structs.

It is a package of this module rather than a `replace` directive on purpose. A
`replace` only applies to the main module being built: anyone importing
`github.com/doze-dev/doze-kafka/server` would have compiled the broker against
upstream kmsg and had the hang back, and `go install …@version` refuses modules
that carry one. `TestBrokerCodeUsesTheInModuleKmsg` keeps both from returning.

The console and the tests still use upstream kmsg through franz-go's client; the
two are wire-compatible and never exchange Go values.

To drop this fork: once upstream merges the equivalent fix, delete internal/kmsg and
point the imports back at `github.com/twmb/franz-go/pkg/kmsg`.
