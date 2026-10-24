# Mutation survivors

Written by `go run ./scripts/mutate report` from `testdata/mutation.json`; edit a
reason there, not here. A mutant is one operator swapped for another. **Lived**:
the tests ran and passed with the code wrong. **Not covered**: no test executes
the line. Both are tests to write. **Timed out** and **setup failed** are runs
that gave no verdict; neither is a kill. **Not viable** mutants do not compile
and say nothing.

The last column of a lived mutant is its written reason when somebody has
examined it. Where it begins *open:* nobody has, and the sentence is the tool's:
what a suite that lets that kind of mutant live does not assert, and the tests
that execute the line without failing.

## Every unit

| Unit | Mutants | Killed | Lived | Not covered | Timed out | Not viable | Setup failed | Open | Tests | Suite | Run | Workers | Platform | Commit | Recorded |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `cmd/doze-kafka` | 309 | 256 | 7 | 6 | 3 | 37 | 0 | 13 | 135 | 55.4 s | 39 min | 2 | darwin/arm64 | `4c1b622` | 2026-10-24 |
| `console` | 2822 | 2232 | 102 | 0 | 21 | 467 | 0 | 0 | 498 | 116.4 s | 480 min | 2 | darwin/arm64 | `019daf1` | 2026-10-25 |
| `internal/broker:admin` | 168 | 158 | 5 | 0 | 0 | 5 | 0 | 0 | 393 | 737.1 s | 119 min | 2 | darwin/arm64 | `f3ef714+changes` | 2026-10-24 |
| `internal/broker:auth` | 190 | 165 | 4 | 3 | 0 | 18 | 0 | 0 | 393 | 857.2 s | 75 min | 2 | darwin/arm64 | `7d7a5dc+changes` | 2026-10-24 |
| `internal/broker:data` | 369 | 335 | 26 | 0 | 0 | 8 | 0 | 0 | 394 | 689.2 s | 118 min | 2 | darwin/arm64 | `f3ef714+changes` | 2026-10-24 |
| `internal/broker:groups` | 749 | 631 | 53 | 2 | 3 | 60 | 0 | 28 | 312 | 809.1 s | 731 min | 2 | darwin/arm64 | `4c92161` | 2026-10-24 |
| `internal/config` | 243 | 227 | 4 | 0 | 0 | 12 | 0 | 0 | 29 | 0.7 s | 7 min | 1 | darwin/arm64 | `4c92161+changes` | 2026-10-24 |
| `internal/protocol` | 608 | 510 | 14 | 0 | 2 | 82 | 0 | 0 | 135 | 11.4 s | 20 min | 2 | darwin/arm64 | `186fd3a` | 2026-10-24 |
| `internal/settings` | 95 | 59 | 0 | 1 | 0 | 35 | 0 | 0 | 15 | 0.4 s | 48 s | 2 | darwin/arm64 | `4c92161+changes` | 2026-10-24 |
| `internal/storage/log` | 1178 | 1043 | 92 | 5 | 13 | 25 | 0 | 41 | 229 | 192.4 s | 401 min | 2 | darwin/arm64 | `db31744` | 2026-10-24 |
| `internal/storage/meta` | 164 | 145 | 15 | 2 | 2 | 0 | 0 | 17 | 37 | 8.3 s | 8 min | 2 | darwin/arm64 | `548b43b` | 2026-10-24 |
| `internal/telemetry` | 162 | 148 | 8 | 0 | 0 | 6 | 0 | 8 | 43 | 0.5 s | 4 min | 1 | darwin/arm64 | `4c92161+changes` | 2026-10-24 |
| `internal/traffic` | 252 | 237 | 10 | 0 | 1 | 4 | 0 | 0 | 70 | 0.5 s | 10 min | 2 | darwin/arm64 | `9cb92d7` | 2026-10-24 |
| `schemaregistry` | 792 | 560 | 102 | 36 | 0 | 94 | 0 | 138 | 34 | 2.7 s | 14 min | 3 | darwin/arm64 | `31f2e5a` | 2026-10-16 |
| **all** | 8101 | 6706 | 442 | 55 | 45 | 853 | 0 | 245 | | | 2026 min | | | | |

## cmd/doze-kafka

### Lived (7)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `cmd/doze-kafka/main.go:193:37` | - became + | `rest := make([]string, 0, len(args)-1)` | equivalent: the expression only sizes the capacity of a slice that is appended to, so len(args)+1 holds the same elements |
| `cmd/doze-kafka/main.go:161:32` | != became == | `if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) && err != http.ErrServerClosed {` | untestable: the line logs a pprof Serve failure other than a closed listener or server; a listener from net.Listen has no other failure a test can cause (read from the code) |
| `cmd/doze-kafka/main.go:161:80` | != became == | `if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) && err != http.ErrServerClosed {` | equivalent: pprof's server is never Shutdown or Closed (the listener is closed instead, which is net.ErrClosed and excluded by the clause before), so Serve never returns http.ErrServerClosed and the clause is true or false alike when the log is reached |
| `cmd/doze-kafka/main.go:598:36` | != became == | `if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {` | untestable: the line logs a console Serve failure other than http.ErrServerClosed; the listener is the one just bound and Serve ends only by httpSrv.Close, so a test cannot cause any other (the mutant that logs on a clean stop is killed by TestAStalledHTTPRequestIsCutOffAfterTenSeconds) |
| `cmd/doze-kafka/names.go:259:17` | * became / | `time.Sleep(20 * time.Millisecond)` | equivalent in result: a sleep of 0 instead of 20ms makes the wait for a front door a busy loop that ends at the same 500ms deadline with the same answer; only CPU use differs |
| `cmd/doze-kafka/registry.go:130:17` | * became / | `time.Sleep(20 * time.Millisecond)` | equivalent in result: same as names.go:259:17, in the schema registry's wait |
| `cmd/doze-kafka/registry.go:60:26` | != became == | `if err := serve(); err != nil && err != http.ErrServerClosed {` | untestable: the line logs a registry Serve failure other than http.ErrServerClosed; the listener is the one just bound and Serve ends only by httpSrv.Close, so a test cannot cause any other (the mutant that logs on a clean stop is killed by TestAStalledHTTPRequestIsCutOffAfterTenSeconds) |

### Timed out (3)

| Where | Change | Line | Reason |
|---|---|---|---|
| `cmd/doze-kafka/cli.go:53:30` | ++ became -- | `for i := 0; i < len(args); i++ {` | hang: the argument loop counts down from 0 and never reaches len(args); every test that parses flags blocks (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `cmd/doze-kafka/cli.go:77:5` | ++ became -- | `i++` | hang: --version N steps back onto the same argument for ever (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `cmd/doze-kafka/main.go:265:10` | != became == | `if err != nil {` | hang: with SASL on, a provisioned console credential returns from run() before serving, and the tests wait for a summary that never comes (read from the code on 2026-10-16; the mutant itself was not run by hand) |

### Not covered (6)

No test executes these lines. By declaration:

- `cmd/doze-kafka/listen.go` `openListener`: 6 (lines 143, 153, 151, 156)

## console

### Lived (102)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `console/access.go:251:27` | != became == | `"HasTraffic": c.traffic != nil, "Refused": refused, "RefusedOver": over,` | equivalent: shellData sets data["HasTraffic"] (and data["HasRegistry"]) on every full page after the handler built its data, so this entry is overwritten before any template reads it |
| `console/backend.go:471:26` | > became >= | `if len(cresp.Resources) > 0 {` | equivalent: DescribeConfigs answers exactly one resource for the one asked, so '> 0' and '>= 0' agree |
| `console/backend.go:262:59` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (topic names), so the comparator is never asked to order equal elements |
| `console/backend.go:542:37` | >= became > | `if p.Partition < 0 \|\| p.Partition >= partitions {` | unreachable: the response holds only the partitions that were asked for (0 to partitions-1), so no partition equals the count |
| `console/backend.go:808:20` | <= became < | `if partitionCount <= 1 {` | equivalent: with one partition the keyed hash and the round robin both give partition 0, so the early return and the general path agree |
| `console/backend.go:710:48` | > became >= | `if reached \|\| fctx.Err() != nil \|\| (len(out) > 0 && pctx.Err() != nil) {` | equivalent: when no record has arrived (len(out) == 0) the poll context is the fetch context itself, so 'fctx.Err() != nil' has already ended the loop |
| `console/backend.go:70:32` | * became / | `d := net.Dialer{Timeout: 10 * time.Second} // franz-go's own default.` | untestable: the 10 s dial timeout of the OnDial path shows only for an address that does not answer, and a test would wait the 10 s |
| `console/backend.go:61:21` | != became == | `if opts.TLSConfig != nil {` | untestable: a unix:// address with a TLS config is a pairing no caller builds (module/serve.go passes none) and franz-go refuses a custom Dialer beside DialTLSConfig, so New fails whenever the branch is taken; the other direction adds a nil DialTLSConfig, which is inert |
| `console/backend_cluster.go:75:43` | > became >= | `if info.Host == "" && len(dresp.Brokers) > 0 {` | equivalent on a single-node broker: the loop above has already filled the host from the one broker, and the fallback would fill the same values |
| `console/backend_cluster.go:91:89` | < became <= | `sort.Slice(info.SupportedAPIs, func(i, j int) bool { return info.SupportedAPIs[i].Name < info.SupportedAPIs[j].Name })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (API names), so the comparator is never asked to order equal elements |
| `console/backend_cluster.go:100:83` | > became >= | `if cresp, err := creq.RequestWith(ctx, b.cl); err == nil && len(cresp.Resources) > 0 && cresp.Resources[0].ErrorCode == 0 {` | equivalent: DescribeConfigs answers exactly one resource for the one asked, so '> 0' and '>= 0' agree |
| `console/backend_cluster.go:70:16` | == became != | `if br.NodeID == dresp.ControllerID {` | equivalent on a single-node broker: DescribeCluster lists the one broker, which is also the controller; the fallback beneath takes the first broker and gives the same node, host and port |
| `console/backend_cluster.go:75:15` | == became != | `if info.Host == "" && len(dresp.Brokers) > 0 {` | equivalent on a single-node broker: the loop above has already filled the host from the one broker, and the fallback would fill the same values |
| `console/backend_cluster.go:75:43` | > became <= | `if info.Host == "" && len(dresp.Brokers) > 0 {` | equivalent on a single-node broker: the loop above has already filled the host from the one broker, and the fallback would fill the same values |
| `console/backend_cluster.go:138:23` | > became >= | `if s.StartTimestamp > 0 {` | equivalent: an open transaction has begun, so its start timestamp is never exactly 0 |
| `console/backend_groups.go:231:24` | < became <= | `return out[i].Topic < out[j].Topic` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique ((topic, partition) pairs), so the comparator is never asked to order equal elements |
| `console/backend_groups.go:233:27` | < became <= | `return out[i].Partition < out[j].Partition` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique ((topic, partition) pairs), so the comparator is never asked to order equal elements |
| `console/backend_groups.go:67:57` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (group ids), so the comparator is never asked to order equal elements |
| `console/backend_groups.go:297:42` | >= became > | `if o.Partition < 0 \|\| int(o.Partition) >= len(bounds[o.Topic]) {` | unreachable: the broker drops a committed offset for a partition that does not exist (CommitOffsets answers it and stores nothing; TestResetGroupOffsets and TestTopicReadersLagPerPartition try it), so no row has a partition number equal to the partition count |
| `console/backend_groups.go:286:10` | ++ became -- | `onTopic++` | equivalent: onTopic is only compared with 0, and a count of -n is as non-zero as a count of n |
| `console/backend_groups.go:499:63` | >= became > | `if o.Topic != topic \|\| o.Partition < 0 \|\| int(o.Partition) >= len(bounds) {` | unreachable: the broker drops a committed offset for a partition that does not exist (CommitOffsets answers it and stores nothing; TestResetGroupOffsets and TestTopicReadersLagPerPartition try it), so no row has a partition number equal to the partition count |
| `console/backend_groups.go:172:30` | > became >= | `if len(m.MemberAssignment) > 0 && a.ReadFrom(m.MemberAssignment) == nil {` | equivalent: an empty member assignment makes ReadFrom fail, so '>= 0' takes the same branch as '> 0' for it |
| `console/confirms.go:589:64` | > became >= | `if add, err := strconv.Atoi(q.Get("add")); err == nil && add > 0 && id == "topic.partitions" {` | equivalent: add=0 gives the same sentence as no add at all (Add is 0 in both) |
| `console/confirms.go:477:21` | < became <= | `if f.DiskOK && i < len(f.Disk.Bases) && f.Disk.Bases[i] >= 0 && f.Disk.Bases[i] < b.Earliest {` | equivalent: Bases holds one entry per partition and i walks the same partitions, so i equals len(Bases) never happens |
| `console/confirms.go:483:63` | < became <= | `sort.Slice(cons, func(i, j int) bool { return cons[i].Group < cons[j].Group })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (group names), so the comparator is never asked to order equal elements |
| `console/confirms.go:182:16` | > became >= | `if len(shown) > 3 {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (names cut to three) |
| `console/confirms.go:168:16` | > became >= | `if len(shown) > 3 {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (names cut to three, 'shown' is already no longer than three) |
| `console/confirms.go:349:17` | > became >= | `if len(shown) > 3 {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (names cut to three) |
| `console/console.go:462:37` | / became * | `m := make(map[string]any, len(kv)/2)` | equivalent: this only sizes a buffer's capacity (a map made for len(kv)/2 entries); the output is the same for any capacity |
| `console/createcommand.go:17:76` | >= became > | `if v, err := strconv.Atoi(strings.TrimSpace(partitions)); err == nil && v >= 1 {` | equivalent: a partition count of exactly 1 gives the default of 1 either way |
| `console/disk.go:54:89` | < became <= | `if off, err := strconv.ParseInt(name, 10, 64); err == nil && (d.Bases[p] < 0 \|\| off < d.Bases[p]) {` | equivalent: two segment files cannot carry the same base offset in their names, and equal bases would give the same value |
| `console/disk.go:112:7` | < became <= | `if f < 10 {` | equivalent: at exactly 10.0 the '< 10' branch writes '10' (floor to a tenth, trailing .0 trimmed) as the other branch does |
| `console/errors.go:157:115` | < became <= | `for j := i; j > 0 && (cands[j].score < cands[j-1].score \|\| (cands[j].score == cands[j-1].score && cands[j].name < cands[j-1].name)); j-- {` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (suggested names, each once), so the comparator is never asked to order equal elements |
| `console/graph.go:103:25` | < became <= | `return label(a.From) < label(b.From)` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (a node's kind and name are one label, and the branch is taken only when the two labels differ), so the comparator is never asked to order equal elements |
| `console/graph.go:106:18` | < became <= | `return a.Kind < b.Kind` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (the branch is taken only when the two kinds differ), so the comparator is never asked to order equal elements |
| `console/graph.go:108:22` | < became <= | `return label(a.To) < label(b.To)` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (an edge is unique by its (from, kind, to)), so the comparator is never asked to order equal elements |
| `console/handlers_cluster.go:59:39` | > became >= | `if view == "overview" && len(topics) > 0 {` | equivalent: with no topics TopicConfigs is asked for no names and answers nothing, so reading it changes nothing |
| `console/handlers_groups.go:197:35` | >= became > | `if b == nil \|\| int(o.Partition) >= len(b) {` | unreachable: the broker drops a committed offset for a partition that does not exist (CommitOffsets answers it and stores nothing; TestResetGroupOffsets and TestTopicReadersLagPerPartition try it), so no row has a partition number equal to the partition count |
| `console/handlers_groups.go:222:37` | < became <= | `if row.Lag > 0 && row.LagPercent < 6 {` | equivalent: a lag bar of exactly 6 percent is set to 6 |
| `console/handlers_groups.go:112:50` | < became <= | `sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (partition numbers of one member, each once), so the comparator is never asked to order equal elements |
| `console/handlers_schemas.go:395:81` | > became >= | `if rest, err := c.reg.Versions(r.Context(), subject); err == nil && len(rest) > 0 {` | equivalent: Versions of a subject with no versions left answers 404, so 'len(rest) > 0' is never reached with 0 |
| `console/handlers_schemas.go:249:34` | < became <= | `for i, j := 0, len(vrows)-1; i < j; i, j = i+1, j-1 {` | equivalent: the loop swaps rows pairwise from both ends; with '<=' it swaps the middle row with itself |
| `console/handlers_schemas.go:271:14` | != became == | `if l.Op != "same" {` | equivalent: the template only tests this count against 0 to say 'no change to the text', and the registry never holds two versions with the same text, so that branch is not reached by either count |
| `console/handlers_schemas.go:208:27` | == became != | `"IsLatest": cur.Version == latestV, "Tab": tab,` | equivalent: no template reads IsLatest |
| `console/handlers_schemas.go:272:14` | ++ became -- | `changed++` | equivalent: the template only tests this count against 0 to say 'no change to the text', which the registry never lets happen (it holds no two versions with the same text), and -n is as non-zero as n |
| `console/handlers_schemas.go:105:23` | <= became < | `for id := s.From; id <= s.To; id++ {` | equivalent: the last id of the span is an id some version uses, so it is never listed as unused whether or not the loop visits it |
| `console/handlers_topics.go:353:16` | > became >= | `if b.Count() > 0 && b.Newest.After(f.Newest) {` | equivalent: an empty partition reports no newest time (the broker answers -1 for its maximum timestamp), so counting it would not move the newest |
| `console/handlers_topics.go:362:101` | > became >= | `if msgs, err := c.be.FetchWindow(ctx, topic, b.Partition, b.Latest-1, 1); err == nil && len(msgs) > 0 {` | equivalent: the record just before the log end exists, so the fetch of it never returns empty; '>= 0' would only differ by indexing an empty slice |
| `console/handlers_topics.go:919:67` | - became + | `msgs, err = c.be.FetchWindow(ctx, topic, partition, from, int(to-from))` | equivalent: a larger limit reads on past the window, and only the offsets inside [from, to) are used |
| `console/handlers_topics.go:933:38` | + became - | `rows := make([]msgView, 0, len(msgs)+len(markers))` | equivalent: this only sizes a buffer's capacity (a slice made for the rows); the output is the same for any capacity |
| `console/handlers_topics.go:918:8` | > became >= | `if to > from {` | equivalent: a window with to == from is empty, and fetching it returns nothing |
| `console/handlers_topics.go:935:23` | < became <= | `for off := from; off < to; off++ {` | equivalent: no record or marker lies at offset 'to', which is outside the window |
| `console/handlers_topics.go:131:111` | != became == | `"Taken": taken, "TakenJSON": string(b), "Prefill": prefill, "PrefillJSON": string(pj), "HasRegistry": c.reg != nil, "MaxParts": c.facts.MaxPartitions,` | equivalent: shellData sets data["HasTraffic"] (and data["HasRegistry"]) on every full page after the handler built its data, so this entry is overwritten before any template reads it |
| `console/handlers_topics.go:261:35` | != became == | `data["HasTraffic"] = c.traffic != nil` | equivalent: shellData sets data["HasTraffic"] (and data["HasRegistry"]) on every full page after the handler built its data, so this entry is overwritten before any template reads it |
| `console/handlers_topics.go:463:63` | >= became > | `if o.Topic != topic \|\| o.Partition < 0 \|\| int(o.Partition) >= len(bounds) {` | unreachable: the broker drops a committed offset for a partition that does not exist (CommitOffsets answers it and stores nothing; TestResetGroupOffsets and TestTopicReadersLagPerPartition try it), so no row has a partition number equal to the partition count |
| `console/handlers_topics.go:1011:78` | - became + | `if cm, err := c.be.FetchWindowCommitted(ctx, topic, partition, from, int(to-from)); err == nil {` | equivalent: a larger limit reads on past the window; the statuses are looked up only for the window's records |
| `console/handlers_topics.go:1010:18` | > became >= | `if len(markers) > 0 \|\| lso < to {` | equivalent: the read-committed read is only made to learn which records are aborted; making it when it is not needed finds none, and not making it when it is needed is what the tests above assert |
| `console/handlers_topics.go:1010:29` | < became <= | `if len(markers) > 0 \|\| lso < to {` | equivalent: the read-committed read is only made to learn which records are aborted; making it when it is not needed finds none, and not making it when it is needed is what the tests above assert |
| `console/handlers_topics.go:1010:29` | < became >= | `if len(markers) > 0 \|\| lso < to {` | equivalent: the read-committed read is only made to learn which records are aborted; making it when it is not needed finds none, and not making it when it is needed is what the tests above assert |
| `console/handlers_topics.go:790:21` | < became <= | `if err != nil \|\| n < 0 \|\| int32(n) >= count {` | equivalent: a partition parameter of 0 is partition 0 whether it is taken as valid or clamped to 0 |
| `console/handlers_topics.go:1042:10` | <= became < | `if size <= 0 {` | equivalent: tickHeight of 0 or less is 6, and the formula gives 6 for a size of 0 |
| `console/handlers_trace.go:207:77` | != became == | `fmt.Fprint(h, j.phase, scanned, j.partsDone, len(j.hits), j.stopped, j.err != nil)` | equivalent: the hash only has to differ between a scan that failed and one that did not, and 'err == nil' differs just as well; its value is compared with itself over time, never with a constant |
| `console/handlers_trace.go:163:17` | > became >= | `if b.Count() > 0 {` | equivalent: an empty partition gives no recent records, so including it changes only which partitions the 64-partition cap spends a slot on, and empty ones sort last |
| `console/handlers_trace.go:169:16` | > became >= | `if len(parts) > traceRecentPartitions {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (cut to 64 partitions) |
| `console/handlers_trace.go:189:14` | > became >= | `if len(out) > traceRecentRows {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (cut to 8 rows) |
| `console/inspector.go:139:11` | != became == | `if v.Cap != "" {` | equivalent: no caller sets Cap on an inspectorView drawn by inspect(); the label comes from the kind |
| `console/inspector.go:267:42` | > became >= | `if i := strings.LastIndex(tp, "-"); i > 0 {` | equivalent: a partition name never starts with '-', so index 0 is never the last '-' |
| `console/jsontint.go:23:16` | + became - | `b.Grow(len(s) + len(s)/2)` | equivalent: this only sizes a buffer's capacity (strings.Builder.Grow); the output is the same for any capacity |
| `console/jsontint.go:23:24` | / became * | `b.Grow(len(s) + len(s)/2)` | equivalent: this only sizes a buffer's capacity (strings.Builder.Grow); the output is the same for any capacity |
| `console/metrics.go:116:9` | >= became > | `case a >= 100:` | equivalent: at exactly 100 the two formats give '100': 'f',0 and 'f',2 after trimming zeros |
| `console/metrics.go:118:9` | >= became > | `case a >= 1:` | equivalent: at exactly 1 the two formats give '1': 'f',2 trimmed and 'g',3 |
| `console/overview.go:201:37` | >= became > | `if b == nil \|\| int(o.Partition) >= len(b) {` | unreachable: the broker drops a committed offset for a partition that does not exist (CommitOffsets answers it and stores nothing; TestResetGroupOffsets and TestTopicReadersLagPerPartition try it), so no row has a partition number equal to the partition count |
| `console/payload.go:282:21` | * became / | `buf.Grow(min(len(b)*2, prettyLimit))` | equivalent: this only sizes a buffer's capacity (bytes.Buffer.Grow); the output is the same for any capacity |
| `console/payload.go:136:17` | > became >= | `if len(sample) > 4096 {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (a sample cut to 4096 bytes) |
| `console/payload.go:188:16` | <= became < | `for i := 1; i <= utf8.UTFMax && i <= len(b); i++ {` | equivalent: a UTF-8 sequence is at most four bytes, so a byte four from the end that starts one starts a complete character and the function returns b whether the loop reaches it or not |
| `console/registry.go:225:16` | > became >= | `if v.Version > out[i].Latest {` | equivalent: a subject has each version once, so two versions are never equal |
| `console/registry.go:229:59` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (subject names), so the comparator is never asked to order equal elements |
| `console/registry.go:350:69` | < became <= | `if e, ok := rc.codecs[id]; ok && (e.err == nil \|\| time.Since(e.at) < 5*time.Second) {` | equivalent: the age of a cached refusal is a clock reading and equals the five-second limit only by accident |
| `console/search.go:68:27` | < became <= | `if before >= 0 && before < latest {` | equivalent: a 'before' equal to the log end is the log end |
| `console/search.go:141:30` | < became <= | `return hits[i].Partition < hits[j].Partition` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (the branch is taken only when the partitions differ), so the comparator is never asked to order equal elements |
| `console/search.go:143:26` | > became >= | `return hits[i].Offset > hits[j].Offset` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (a record is one (partition, offset)), so the comparator is never asked to order equal elements |
| `console/trace_scan.go:403:25` | < became <= | `return hits[a].Topic < hits[b].Topic` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (the branch is taken only when the topics differ), so the comparator is never asked to order equal elements |
| `console/trace_scan.go:406:29` | < became <= | `return hits[a].Partition < hits[b].Partition` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (the branch is taken only when the partitions differ), so the comparator is never asked to order equal elements |
| `console/trace_scan.go:408:25` | < became <= | `return hits[a].Offset < hits[b].Offset` | equivalent: '<' against '<=' in a sort comparator differs only for two equal keys, and the keys here are unique (a record is one (topic, partition, offset)), so the comparator is never asked to order equal elements |
| `console/trace_scan.go:249:25` | < became <= | `return plan[a].size() < plan[b].size()` | equivalent: the plan is built from a map, so the order of two ranges of equal size is whichever the map gave; either order is correct, and no test can ask for one |
| `console/trace_scan.go:339:25` | >= became > | `if !ok \|\| reached[k] >= rg.To \|\| r.Offset >= rg.To {` | equivalent: reached[k] >= To implies the record's offset is >= To from the second condition (reached is the last offset read plus one, never above To) |
| `console/trace_scan.go:342:7` | ++ became -- | `got++` | equivalent: got is only compared with 0, and a negative count is as non-zero as a positive one |
| `console/trace_scan.go:121:54` | > became >= | `old := !j.finished.IsZero() && now.Sub(j.finished) > traceJobTTL` | equivalent: the age of a finished scan is a clock reading and equals the limit only by accident |
| `console/trace_scan.go:129:55` | < became <= | `fresh := j.finished.IsZero() \|\| now.Sub(j.finished) < traceReuseFor` | equivalent: the age of a finished scan is a clock reading and equals the limit only by accident |
| `console/trace_scan.go:105:23` | * became / | `tracePollEvery = 600 * time.Millisecond` | equivalent: tracePollEvery is declared and nothing reads it |
| `console/trace_scan.go:477:9` | > became >= | `if pre > 0 {` | equivalent: a leading group of zero digits writes nothing |
| `console/traffic.go:475:10` | > became >= | `case n > 1:` | equivalent: the n == 1 case above already took one topic, so '> 1' and '>= 1' choose the same for every n |
| `console/traffic.go:572:9` | < became <= | `case d < 0:` | equivalent: a duration of exactly 0 gives '0 ms' in both branches |
| `console/traffic_inspect.go:527:17` | > became >= | `if len(drawn) > sparkBars {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (samples cut to the last 12) |
| `console/traffic_inspect.go:530:17` | > became >= | `if len(drawn) > 1 {` | equivalent: a series of one sample has a span of zero, which does not raise the longest span |
| `console/traffic_inspect.go:560:18` | > became >= | `if len(m.Worth) > 4 && !all {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (four rows cut to four, and nothing is added to the count of those not shown) |
| `console/traffic_inspect.go:564:25` | > became >= | `if !all && len(m.Rest) > restShown {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (four rows cut to four, and nothing is added to the count of those not shown) |
| `console/traffic_inspect.go:273:34` | != became == | `if e.ClientID != "" \|\| e.Remote != "" {` | equivalent: the template draws the link this feeds only for a request with a client id, and for those both comparisons agree |
| `console/traffic_inspect.go:589:32` | < became <= | `if math.Abs(s-math.Round(s)) < 0.05 \|\| s >= 10 {` | equivalent: the two bounds are float comparisons that equal each other only by accident (0.05 apart from a rounded second; 10 s exactly is a whole second either way) |
| `console/traffic_inspect.go:589:44` | >= became > | `if math.Abs(s-math.Round(s)) < 0.05 \|\| s >= 10 {` | equivalent: the two bounds are float comparisons that equal each other only by accident (0.05 apart from a rounded second; 10 s exactly is a whole second either way) |
| `console/traffic_inspect.go:461:18` | > became >= | `if len(samples) > sparkBars {` | equivalent: the comparison only guards a slice cut that gives the same result whether or not it runs at exactly this length (samples cut to the last 12) |
| `console/traffic_inspect.go:474:9` | > became >= | `if hi > 0 && !math.IsNaN(s.V) && !math.IsInf(s.V, 0) {` | untestable here: with every value 0 the height formula divides 0 by 0, and int(NaN) is 0 on arm64 (where this runs) and the same bar of 2 px results; on amd64 it is a different integer |

### Timed out (21)

| Where | Change | Line | Reason |
|---|---|---|---|
| `console/backend.go:389:38` | ++ became -- | `for p := int32(0); p < partitions; p++ {` | hang: the loop counter runs backwards and never reaches its bound (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/errors.go:156:31` | ++ became -- | `for i := 1; i < len(cands); i++ {` | hang: the loop counter runs backwards and never reaches its bound (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/handlers_schemas.go:105:34` | ++ became -- | `for id := s.From; id <= s.To; id++ {` | hang: the id counter of the unused-ids loop runs backwards and never reaches the last id |
| `console/handlers_topics.go:935:23` | < became >= | `for off := from; off < to; off++ {` | hang: the offset loop runs while off >= to, which never ends for a range that starts before its end (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/handlers_topics.go:935:32` | ++ became -- | `for off := from; off < to; off++ {` | hang: the loop counter runs backwards and never reaches its bound (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/handlers_topics.go:996:32` | ++ became -- | `for off := from; off < to; off++ {` | hang: the loop counter runs backwards and never reaches its bound (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:67:23` | >= became > | `case c == '-' \|\| (c >= '0' && c <= '9'):` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:67:35` | <= became < | `case c == '-' \|\| (c >= '0' && c <= '9'):` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:67:10` | == became != | `case c == '-' \|\| (c >= '0' && c <= '9'):` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:67:23` | >= became < | `case c == '-' \|\| (c >= '0' && c <= '9'):` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:67:35` | <= became > | `case c == '-' \|\| (c >= '0' && c <= '9'):` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:69:10` | < became >= | `for j < len(s) && strings.ContainsRune("-+.eE0123456789", rune(s[j])) {` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:76:10` | < became >= | `for j < len(s) && strings.ContainsRune("{}[],:", rune(s[j])) {` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:83:10` | < became >= | `for j < len(s) && s[j] != '"' && !strings.ContainsRune("{}[],:", rune(s[j])) &&` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:83:27` | != became == | `for j < len(s) && s[j] != '"' && !strings.ContainsRune("{}[],:", rune(s[j])) &&` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:84:12` | == became != | `!(s[j] == '-' \|\| (s[j] >= '0' && s[j] <= '9')) {` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:84:28` | >= became < | `!(s[j] == '-' \|\| (s[j] >= '0' && s[j] <= '9')) {` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:84:43` | <= became > | `!(s[j] == '-' \|\| (s[j] >= '0' && s[j] <= '9')) {` | hang: the JSON tinter's scan stops advancing j (or starts a token it never leaves), so the page render loops on any decoded value (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/jsontint.go:49:7` | ++ became -- | `j++` | hang: the scan of a string literal steps back instead of forward over an escape and never leaves it |
| `console/trace_scan.go:135:19` | >= became < | `for len(js.byID) >= traceJobsKept { // drop the oldest.` | hang: the drop-the-oldest loop runs while fewer than the cap are kept, and never empties (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `console/trace_scan.go:166:41` | ++ became -- | `for p := int32(0); p < t.Partitions; p++ {` | hang: the partition counter of the ListOffsets request runs backwards and never reaches the topic's partition count |

## internal/broker:admin

### Lived (5)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/broker/createpartitions.go:68:15` | < became <= | `case t.Count < cur.Partitions:` | equivalent: the equal case is the first case of the switch (t.Count == cur.Partitions), so < is only reached with the two counts different and <= answers the same |
| `internal/broker/topiclimits.go:113:95` | > became >= | `if v, err := strconv.ParseInt(topicConfig["min.compaction.lag.ms"], 10, 64); err == nil && v > 0 {` | equivalent: v > 0 and v >= 0 differ only at v = 0, where MinLag = millis(0) = 0 is the value it already has |
| `internal/broker/topiclimits.go:116:95` | > became >= | `if v, err := strconv.ParseInt(topicConfig["max.compaction.lag.ms"], 10, 64); err == nil && v > 0 {` | equivalent: v > 0 and v >= 0 differ only at v = 0, where MaxLag = millis(0) = 0 is the value it already has |
| `internal/broker/topics.go:338:62` | != became == | `if err := b.logs.RemoveTopic(name, topic.Partitions); err != nil {` | log only: when RemoveTopic fails after the registry entry is gone the only effect is a Warn line, and making it fail needs a partition directory the process cannot remove, which a test run as root (as the container runs them) cannot arrange because chmod does not stop root |
| `internal/broker/topics.go:288:61` | != became == | `if rerr := b.logs.RemoveTopic(t.Name, t.Partitions); rerr != nil {` | log only: when removing the data of a topic whose directories could not be made fails, the only effect is a Warn line ("rollback of partition data failed"); RemoveTopic fails only on a directory the process cannot remove, which a test run as root cannot arrange because chmod does not stop root |

## internal/broker:auth

### Lived (4)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/broker/acls.go:280:16` | < became <= | `return a.rt < b.rt` | equivalent: the comparison is reached only inside if a.rt != b.rt, where the two differ, so < and <= order every pair alike |
| `internal/broker/acls.go:283:18` | < became <= | `return a.name < b.name` | equivalent: the comparison is reached only inside if a.name != b.name, where the two differ, so < and <= order every pair alike |
| `internal/broker/acls.go:285:15` | < became <= | `return a.pt < b.pt` | equivalent: the keys sorted are the keys of a map (resource type, name, pattern type), so no two are equal and < and <= order every pair alike |
| `internal/broker/broker.go:291:45` | > became >= | `if t, ok := a.(*net.TCPAddr); ok && t.Port > 0 {` | equivalent: storing a bound port of 0 stores the value an unset port has, and advertisedPort ignores a bound port of 0 or less |

### Not covered (3)

No test executes these lines. By declaration:

- `internal/broker/auth.go` `(*Broker).handleSASLAuthenticate`: 2 (lines 223)
- `internal/broker/scram.go` `(*authenticator).scramFirst`: 1 (lines 239)

## internal/broker:data

### Lived (26)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/broker/deleterecords.go:37:47` | >= became > | `case !ok \|\| p.Partition < 0 \|\| p.Partition >= topic.Partitions:` | equivalent: with > a partition number equal to the partition count is not caught here but reaches Manager.DeleteBefore, which finds no directory for it and answers ErrNoSuchPartition, mapped to the same UNKNOWN_TOPIC_OR_PARTITION with the same -1 low watermark |
| `internal/broker/fetch.go:132:51` | >= became > | `if !ok \|\| part.Partition < 0 \|\| part.Partition >= topic.Partitions {` | equivalent: with > a partition number equal to the count reaches readPartition, whose ExistingPartition answers ErrNoSuchPartition, which sets the same UNKNOWN_TOPIC_OR_PARTITION and nothing else, as this branch does |
| `internal/broker/fetch.go:65:42` | <= became < | `if failed \|\| total >= minBytes \|\| wait <= 0 \|\| !time.Now().Before(deadline) {` | equivalent: a wait of 0 sets the deadline to the moment the request was read, so !time.Now().Before(deadline) is already true on the same pass and the answer, and when it is sent, are the same |
| `internal/broker/fetch.go:190:14` | <= became < | `if maxBytes <= 0 {` | equivalent: with < a partition limit of 0 skips the fall back to the request MaxBytes, but newFetchBudget(fr.MaxBytes) caps the read to that same number two statements later, and with no request limit the default applies at the next test |
| `internal/broker/fetch.go:197:14` | > became >= | `if maxBytes > budget.remaining {` | equivalent: at equality the assignment maxBytes = budget.remaining changes nothing |
| `internal/broker/fetch.go:200:14` | <= became < | `if maxBytes <= 0 {` | equivalent as a client sees it: reached with 0 only when the budget ran out exactly, after an earlier partition gave data; the log layer reads a maxBytes of 0 as no limit, but the batch it returns is dropped by the gaveData check just below, so the partition answers the same apart from the aborted-transaction list beside records that are not sent |
| `internal/broker/fetch.go:103:41` | + became - | `return req.ReplySized(resp, recordBytes+256+96*parts)` | equivalent: only the capacity hint of the encode buffer; EncodeResponseSized clamps it to at least 128 and a different capacity never changes the bytes |
| `internal/broker/fetch.go:103:45` | + became - | `return req.ReplySized(resp, recordBytes+256+96*parts)` | equivalent: only the capacity hint of the encode buffer; EncodeResponseSized clamps it to at least 128 and a different capacity never changes the bytes |
| `internal/broker/fetch.go:169:40` | - became + | `return &fetchBudget{remaining: 1<<62 - 1}` | equivalent: 1<<62-1 and 1<<62+1 are both a budget no fetch can reach (4.6e18 bytes) |
| `internal/broker/idempotency.go:245:7` | > became >= | `if n > len(all) {` | equivalent: at n == len(all) the assignment n = len(all) is a no-op |
| `internal/broker/idempotency.go:203:20` | > became >= | `if len(st.recent) > maxTrackedBatches {` | equivalent: at exactly maxTrackedBatches the slice st.recent[len-5:] is the whole slice |
| `internal/broker/listoffsets.go:143:10` | - became + | `return -1, -1, false, nil` | equivalent: when found is false the caller ignores the offset and time returned (it answers -1, or the log end for v0, itself), so their sign is never read |
| `internal/broker/listoffsets.go:143:14` | - became + | `return -1, -1, false, nil` | equivalent: when found is false the caller ignores the offset and time returned (it answers -1, or the log end for v0, itself), so their sign is never read |
| `internal/broker/produce.go:386:39` | + became - | `if !ok \|\| hdr.TotalBytes <= 0 \|\| pos+hdr.TotalBytes > len(records) {` | equivalent: pos is 0, because CheckProduce admits exactly one batch per partition and the loop runs once; pos-TotalBytes and pos+TotalBytes are both compared with a length no smaller than TotalBytes |
| `internal/broker/produce.go:386:28` | <= became < | `if !ok \|\| hdr.TotalBytes <= 0 \|\| pos+hdr.TotalBytes > len(records) {` | equivalent: ParseBatch succeeds only for a batch of at least its 61-byte header, so TotalBytes is never 0 |
| `internal/broker/produce.go:403:19` | < became <= | `if firstOffset < 0 {` | equivalent: the loop runs once (CheckProduce admits exactly one batch per partition), so firstOffset is -1 when it is tested, never 0 |
| `internal/broker/produce.go:409:19` | < became <= | `if firstOffset < 0 {` | equivalent: the loop runs once (CheckProduce admits exactly one batch per partition), so firstOffset is -1 when it is tested, never 0 |
| `internal/broker/produce.go:383:45` | - became + | `var firstOffset, duplicateTime int64 = -1, -1` | equivalent: a dead initial value; firstOffset is set by the one batch before it is read, and duplicateTime is overwritten with the append time by appendRecords when a batch was written, or set from the stored batch when it is a duplicate |
| `internal/broker/produce.go:360:22` | > became >= | `return len(records) > 0` | equivalent: records is never empty here, since CheckProduce admits only a whole batch |
| `internal/broker/produce.go:358:21` | - became + | `rp.LogAppendTime = -1 // CreateTime: the record's own timestamp stands.` | equivalent: a dead store; appendRecords sets rp.LogAppendTime from appendTime (-1 under CreateTime) whenever the append succeeded, which overwrites it |
| `internal/broker/txn.go:422:47` | - became + | `expired := e.rec.State == txnOngoing && now-e.rec.UpdatedMs > int64(e.rec.TimeoutMs)` | equivalent: expired is a pre-filter read under the read lock; the same condition is tested again under the write lock before anything is aborted, so changing the pre-filter alone leaves the second test to decide |
| `internal/broker/txn.go:445:43` | - became + | `if e.rec.State == txnOngoing && nowMs()-e.rec.UpdatedMs > int64(e.rec.TimeoutMs) {` | equivalent: a transaction within its time is skipped by the pre-filter (if !expired { continue }) before this re-check under the write lock is reached, so the re-check only repeats it; only a change between the two tests, on the loop own goroutine alone, could show it |
| `internal/broker/txn.go:422:64` | > became >= | `expired := e.rec.State == txnOngoing && now-e.rec.UpdatedMs > int64(e.rec.TimeoutMs)` | equivalent: differs only when the elapsed time equals the timeout to the millisecond, which the loop reads from its own clock so a test cannot arrange; and the same condition is tested again under the write lock |
| `internal/broker/txn.go:445:60` | > became >= | `if e.rec.State == txnOngoing && nowMs()-e.rec.UpdatedMs > int64(e.rec.TimeoutMs) {` | equivalent: differs only when the elapsed time equals the timeout to the millisecond, which the loop reads from its own clock so a test cannot arrange |
| `internal/broker/txnapis.go:410:65` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].ProducerID < out[j].ProducerID })` | equivalent: producer ids are unique in the list, so no two elements compare equal and < and <= order every pair alike |
| `internal/broker/txnapis.go:356:72` | < became <= | `sort.Slice(rows, func(i, j int) bool { return rows[i].TransactionalID < rows[j].TransactionalID })` | equivalent: transactional ids are unique in the list, so no two rows compare equal and < and <= order every pair alike |

## internal/broker:groups

### Lived (53)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/broker/group.go:188:30` | <= became < | `return ok && time.Since(at) <= pendingMemberTTL` | equivalent in practice: time.Since(at) <= pendingMemberTTL differs from < only when the elapsed time is exactly 5 minutes to the nanosecond; the clock is time.Now with a monotonic reading, so no test can set it, and a real id is never that old to the nanosecond. |
| `internal/broker/group.go:465:30` | > became >= | `if r.RebalanceTimeoutMillis > 0 {` | equivalent: r.RebalanceTimeoutMillis >= 0 differs only at 0, where it assigns 0 * time.Millisecond = 0 to a field that is already 0. |
| `internal/broker/group.go:479:34` | > became >= | `if wasEmpty && gm.initialDelay > 0 {` | equivalent: with initialDelay 0 the mutant holds with a timer of 0, which completes the join at once on the timer's goroutine; the original completes it on the join's own. The join's answer, generation and state are the same. |
| `internal/broker/group.go:534:30` | > became >= | `if r.RebalanceTimeoutMillis > 0 {` | equivalent: r.RebalanceTimeoutMillis >= 0 differs only at 0, where it assigns 0 to the new member's rebalanceTimeout, which is already its zero value. |
| `internal/broker/group848.go:629:81` | < became <= | `if len(g.members) == 0 \|\| g.targetEpoch >= g.epoch \|\| time.Since(g.assignedAt) < m.assignInterval {` | equivalent in practice: time.Since(g.assignedAt) < m.assignInterval differs from <= only when the time since the last computation is exactly the interval to the nanosecond (with an interval of 0, exactly 0 ns); the clock is time.Now with a monotonic reading, which no test can hold still. |
| `internal/broker/group848.go:470:56` | < became <= | `case r.MemberEpoch == mem.prevEpoch && r.MemberEpoch < mem.epoch:` | equivalent: this case is reached only when r.MemberEpoch != mem.epoch (the case above took equal), so r.MemberEpoch < mem.epoch and r.MemberEpoch <= mem.epoch are the same condition. |
| `internal/broker/group848.go:112:71` | < became <= | `sort.Slice(out, func(i, j int) bool { return string(out[i].topic[:]) < string(out[j].topic[:]) })` | equivalent: the topic ids sorted are the distinct keys of a map, so < and <= differ only for equal ids, which do not occur. |
| `internal/broker/group848.go:252:10` | < became <= | `if sent < known {` | equivalent: staleEpoch is called only for an epoch that is not the member's current one (r.MemberEpoch != mem.epoch), so sent == known never reaches it and < and <= give the same word. |
| `internal/broker/group848_assign.go:45:40` | <= became < | `if err != nil \|\| !ok \|\| t.Partitions <= 0 {` | equivalent: a topic with no partitions (t.Partitions <= 0 vs < 0) gives the assignors nothing to assign whether it is listed or skipped; a topic cannot be created with none. |
| `internal/broker/group848_assign.go:60:59` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })` | equivalent: the topics are sorted by name with < or <=; names are unique, so the two differ only for equal names, which do not occur. |
| `internal/broker/group848_assign.go:197:35` | < became <= | `return string(out[i].topic[:]) < string(out[j].topic[:])` | equivalent: sortedParts's order is not observable. The sticky loop keeps every partition of a member's previous set that nobody ranked earlier already owns, and which of a member's own partitions is visited first changes neither the set it keeps nor the counts. |
| `internal/broker/group848_assign.go:199:22` | < became <= | `return out[i].part < out[j].part` | equivalent: sortedParts's order is not observable. The sticky loop keeps every partition of a member's previous set that nobody ranked earlier already owns, and which of a member's own partitions is visited first changes neither the set it keeps nor the counts. |
| `internal/broker/group848_assign.go:196:19` | != became == | `if out[i].topic != out[j].topic {` | equivalent: sortedParts's order is not observable. The sticky loop keeps every partition of a member's previous set that nobody ranked earlier already owns, and which of a member's own partitions is visited first changes neither the set it keeps nor the counts. |
| `internal/broker/group848_assign.go:197:35` | < became >= | `return string(out[i].topic[:]) < string(out[j].topic[:])` | equivalent: sortedParts's order is not observable. The sticky loop keeps every partition of a member's previous set that nobody ranked earlier already owns, and which of a member's own partitions is visited first changes neither the set it keeps nor the counts. |
| `internal/broker/group848_assign.go:199:22` | < became >= | `return out[i].part < out[j].part` | equivalent: sortedParts's order is not observable. The sticky loop keeps every partition of a member's previous set that nobody ranked earlier already owns, and which of a member's own partitions is visited first changes neither the set it keeps nor the counts. |
| `internal/broker/groupadmin.go:65:16` | == became != | `if g.members == 0 && b.claims.ownerOf(g.id) == protoClassic {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAGroupWithNoMembersAndNoOffsetsIsGoneAtTheNextSweep, TestAdmin_ListGroups, TestGroupAdmin_ConsumerGroupStateIsReal (and 3 more) |
| `internal/broker/groupadmin.go:72:31` | == became != | `if known[g.id] && g.members == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAGroupWithNoMembersAndNoOffsetsIsGoneAtTheNextSweep, TestAdmin_ListGroups, TestALongNameInAClassicJoinIsRefusedAndNotKept (and 7 more) |
| `internal/broker/groupmigrate.go:518:10` | == became != | `if id == memberID \|\| other.classic == nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 13 more) |
| `internal/broker/groupmigrate.go:688:16` | <= became < | `if timeout <= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupClassicMemberThatNeverRejoinsIsRemoved, TestACooperativeClassicMemberKeepsWhatItOwnsUntilItGivesItUp, TestAnEagerClassicMemberGivesUpEverythingWhenItRejoins (and 5 more) |
| `internal/broker/groupmigrate.go:708:21` | > became >= | `if len(g.members) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupClassicMemberThatNeverRejoinsIsRemoved |
| `internal/broker/groupmigrate.go:463:140` | * became / | `mem.classic = &classicSeat{protocols: protocols, protocol: protocol, version: sub.version, session: time.Duration(r.SessionTimeoutMillis) * time.Millisecond}` | *open:* no test asserts the value this expression computes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:464:65` | * became / | `mem.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond` | *open:* no test asserts the value this expression computes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:422:81` | * became / | `if err := m.liveness.joining(g.id, true, time.Duration(r.SessionTimeoutMillis)*time.Millisecond, "consumer"); err != nil {` | *open:* no test asserts the value this expression computes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:369:82` | < became <= | `if session := time.Duration(r.SessionTimeoutMillis) * time.Millisecond; session < minSessionTimeout \|\| session > maxSessionTimeout {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 13 more) |
| `internal/broker/groupmigrate.go:369:113` | > became >= | `if session := time.Duration(r.SessionTimeoutMillis) * time.Millisecond; session < minSessionTimeout \|\| session > maxSessionTimeout {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 13 more) |
| `internal/broker/groupmigrate.go:391:23` | >= became > | `case r.GetVersion() >= 4:` | *open:* no test puts the two sides equal and asserts which way the comparison goes |
| `internal/broker/groupmigrate.go:465:26` | <= became < | `if mem.rebalanceTimeout <= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:439:31` | != became == | `changed = predecessor.regex != nil` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAStaticClassicMemberRestartingInAConsumerGroupIsNotSeatedTwice, TestAStaticClassicMemberReplacesAConsumerProtocolHolderOfItsInstance, TestMixedGroup_StaticClassicMemberRestartingHasOneSeat |
| `internal/broker/groupmigrate.go:465:26` | <= became > | `if mem.rebalanceTimeout <= 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:367:72` | - became + | `return joinResult{errorCode: code, memberID: r.MemberID, generation: -1}, true` | *open:* no test asserts the sign of this value; reached by TestMixedGroupRefusals, TestAJoinIsRefusedWhenTheGroupCannotBeRecordedAsLive |
| `internal/broker/groupmigrate.go:395:87` | - became + | `return joinResult{errorCode: kerr.MemberIDRequired.Code, memberID: id, generation: -1}, true` | *open:* no test asserts the sign of this value; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 3 more) |
| `internal/broker/groupmigrate.go:405:74` | - became + | `return joinResult{errorCode: kerr.UnknownServerError.Code, generation: -1}, true` | *open:* no test asserts the sign of this value; reached by TestMixedGroupRefusals, TestGroupCoordinators_ConcurrentFirstJoinsCannotBothWin, TestGroupCoordinators_ShareNoGroupID |
| `internal/broker/groupmigrate.go:414:72` | - became + | `return joinResult{errorCode: kerr.FencedInstanceID.Code, generation: -1}, true` | *open:* no test asserts the sign of this value; reached by TestAStaticClassicMemberRestartingInAConsumerGroupIsNotSeatedTwice |
| `internal/broker/groupmigrate.go:666:21` | > became >= | `if len(g.members) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestMixedGroupRefusals, TestAStaticClassicMemberRestartingInAConsumerGroupIsNotSeatedTwice |
| `internal/broker/groupmigrate.go:666:21` | > became <= | `if len(g.members) > 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestMixedGroupRefusals, TestAStaticClassicMemberRestartingInAConsumerGroupIsNotSeatedTwice |
| `internal/broker/groupmigrate.go:720:27` | - became + | `cutoff := time.Now().Add(-pendingMemberTTL)` | *open:* no test asserts the sign of this value; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 3 more) |
| `internal/broker/groupmigrate.go:747:27` | == became != | `if len(m.pending[group]) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestMixedGroupRefusals, TestMixedGroupSessionExpiryAndDowngrade, TestMixedGroupSeatsOfSeveralGroups (and 11 more) |
| `internal/broker/groupmigrate.go:161:82` | + became - | `g := &group{id: id, state: statePreparingRebalance, generation: snap.generation + 1, protocolType: "consumer",` | *open:* no test asserts the value this expression computes; reached by TestMixedGroupSessionExpiryAndDowngrade, TestAStaticClassicMemberReplacesAConsumerProtocolHolderOfItsInstance, TestOffsetRetentionSweepRacesWithMembersComingAndGoing (and 3 more) |
| `internal/broker/groupmigrate.go:168:45` | != became == | `if sm.instanceID != nil && *sm.instanceID != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestMixedGroupSessionExpiryAndDowngrade, TestAStaticClassicMemberReplacesAConsumerProtocolHolderOfItsInstance, TestOffsetRetentionSweepRacesWithMembersComingAndGoing (and 3 more) |
| `internal/broker/groupmigrate.go:190:24` | / became * | `if n < 0 \|\| n > len(b)/6 {` | equivalent: the n > len(b)/6 bound only refuses early; an n that passes the loosened bound (up to len(b)*6) runs the loop out of bytes and the loop's own length checks return nil, the same answer, so no input tells them apart. |
| `internal/broker/groupmigrate.go:195:13` | < became <= | `if len(b) < 2 {` | equivalent: with exactly 2 bytes left the original reads the 2-byte name length and then fails len(b) < l+4 (b is empty), returning nil; the mutant returns nil at once. Same result for every input. |
| `internal/broker/groupmigrate.go:270:12` | < became <= | `if len(b) < 4 {` | equivalent: with exactly 4 bytes left the original reads the user-data length and then hands decodeClassicAssignment only {0,0} (under 6 bytes), which is nil, so it returns false; the mutant returns false directly. |
| `internal/broker/groupmigrate.go:273:52` | > became >= | `if n := int(int32(binary.BigEndian.Uint32(b))); n > 0 { // user data; -1 is none.` | equivalent: n == 0 means no user data; the original skips nothing, the mutant does b = b[0:], the same slice. |
| `internal/broker/groupmigrate.go:274:8` | > became >= | `if n > len(b)-4 {` | equivalent: when n == len(b)-4 the original slices b = b[n:] leaving 4 bytes, then b[4:] is empty and decodeClassicAssignment({0,0}) is nil, so it returns false; the mutant returns false directly. |
| `internal/broker/groups.go:188:11` | == became != | `if code == errNone {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestGroupCoordinators_MembersOfBothProtocolsShareAGroup, TestMixedGroup_CooperativeClassicMemberAndConsumerProtocolMemberNeverShareAPartition, TestMixedGroup_StaticClassicMemberRestartingHasOneSeat |
| `internal/broker/groups848.go:64:27` | == became != | `if !ok \|\| (len(members) == 0 && b.claims.ownerOf(group) == protoClassic) {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAdmin_ConsumerGroupDescribe, TestEveryServedAPIIsAuthorized, TestGroupAdmin_ConsumerGroupStateIsReal (and 11 more) |
| `internal/broker/groups848.go:43:22` | >= became > | `if res.memberEpoch >= 0 {` | equivalent: a successful answer is never at member epoch 0 (a joining member is answered at the group's epoch, 2 or more, and a leaving one at -1 or -2), so >= 0 and > 0 give the same interval. |
| `internal/broker/grouptimer.go:37:7` | ++ became -- | `e.gen++` | equivalent: expiry.gen only has to differ between armings; counting down gives values as distinct as counting up (a uint64 wraps only after 2^64 armings), and every use compares for equality. |
| `internal/broker/offsetexpiry.go:384:43` | >= became > | `return !live(topic) && now-committedAt >= retention` | untested boundary: the offset is exactly the retention old (to the millisecond). No recorded claim or doc says whether it expires at or after that instant (docs/consumer-groups.md says only 'after --offsets-retention'; the recordings read the offset well before and well after the retention), and a test would pin an unrecorded Kafka behaviour from memory. |
| `internal/broker/offsetexpiry.go:391:69` | >= became > | `return func(string, int32, int64) bool { return now-st.EmptySince >= retention }` | untested boundary: the offset is exactly the retention old (to the millisecond). No recorded claim or doc says whether it expires at or after that instant (docs/consumer-groups.md says only 'after --offsets-retention'; the recordings read the offset well before and well after the retention), and a test would pin an unrecorded Kafka behaviour from memory. |
| `internal/broker/offsetexpiry.go:393:83` | >= became > | `return func(_ string, _ int32, committedAt int64) bool { return now-committedAt >= retention }` | untested boundary: the offset is exactly the retention old (to the millisecond). No recorded claim or doc says whether it expires at or after that instant (docs/consumer-groups.md says only 'after --offsets-retention'; the recordings read the offset well before and well after the retention), and a test would pin an unrecorded Kafka behaviour from memory. |
| `internal/broker/offsetexpiry.go:349:9` | < became <= | `if now < gone {` | untested boundary: a presumed group's session ends exactly at the millisecond now == started+session. No claim says whether it is empty at that instant or the next (R11f says 'once its session has passed'); pinning one would be pinning an unrecorded behaviour. |
| `internal/broker/offsetexpiry.go:493:24` | / became * | `if n < 0 \|\| n > len(b)/2 {` | equivalent: the n > len(b)/2 bound only refuses early; an n that passes the loosened bound runs the loop out of bytes and the loop's own len(b) < 2 and len(b) < l checks return nil, false, the same answer. |

### Timed out (3)

| Where | Change | Line | Reason |
|---|---|---|---|
| `internal/broker/group848_assign.go:122:41` | ++ became -- | `for p := int32(0); p < t.partitions; p++ {` | unkillable by a failing test: p-- makes the loop for p < t.partitions never end, appending a partition per turn without bound, so a test can only end in the timeout (and in memory growth, which is why it was not run here). Any assignUniform test reaching it hangs. |
| `internal/broker/offsetexpiry.go:369:32` | != became == | `if g := b.cg848.groups[id]; g != nil && len(g.members) > 0 {` | the mutant makes every sweep of a group of the other protocol dereference nil with the coordinator locks held; the test then cannot clean up (Broker.Stop waits for those locks) so the run ends by the timeout, never by a failing test. Every offsets-retention sweep test kills it by hanging (checked: TestOffsetRetentionStandaloneCountsFromTheCommit hangs under it); the engine reports a hang as timedOut. |
| `internal/broker/offsetexpiry.go:371:40` | != became == | `} else if g := b.groups.groups[id]; g != nil && len(g.members) > 0 {` | the mutant makes every sweep of a group of the other protocol dereference nil with the coordinator locks held; the test then cannot clean up (Broker.Stop waits for those locks) so the run ends by the timeout, never by a failing test. Every offsets-retention sweep test kills it by hanging (checked: TestOffsetRetentionStandaloneCountsFromTheCommit hangs under it); the engine reports a hang as timedOut. |

### Not covered (2)

No test executes these lines. By declaration:

- `internal/broker/groupmigrate.go` `(*cg848Manager).seatIssued`: 2 (lines 740)

## internal/config

### Lived (4)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/config/config.go:364:44` | - became + | `if !e.IsDir() \|\| i <= 0 \|\| i == len(name)-1 {` | equivalent: i == len(name)+1 is never true, and a name ending in '-' then fails strconv.Atoi("") and is skipped the same way (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `internal/config/file.go:355:32` | > became >= | `if len(cfg.ConsoleAllowHosts) > 0 {` | equivalent: with an empty list, assigning it or not writes the same file (the encoder omits an empty array) (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `internal/config/file.go:364:28` | > became >= | `if len(cfg.ACLSuperUsers) > 0 {` | equivalent: as file.go:282, for acl-super-user (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `internal/config/file.go:370:29` | > became >= | `if len(cfg.SASLMechanisms) > 0 {` | equivalent: as file.go:282, for sasl-mechanisms (read from the code on 2026-10-16; the mutant itself was not run by hand) |

## internal/protocol

### Lived (14)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/protocol/codec.go:80:9` | <= became < | `if adv <= 0 {` | equivalent: binary.Uvarint returns 0 only for an empty or unfinished varint and the buffer is then left as it is; with adv == 0 the count read is 0, so the loop does not run and the same b comes back (read from the code) |
| `internal/protocol/codec.go:85:38` | <= became < | `if _, adv = binary.Uvarint(b); adv <= 0 { // tag` | equivalent: with adv == 0 for a tag the buffer is unchanged, the size read right after it gets adv2 == 0 from the same bytes and returns the same b (read from the code) |
| `internal/protocol/codec.go:90:11` | <= became < | `if adv2 <= 0 {` | equivalent: with adv2 == 0 the size is 0 and the buffer unchanged, so the entry consumes nothing; the next tag read gets adv == 0 from the same bytes and returns the same b, or the count runs out and b comes back (read from the code) |
| `internal/protocol/conn.go:155:15` | > became >= | `if len(name) > MaxClientSoftware+1 {` | equivalent: at exactly MaxClientSoftware+1 bytes, name[:MaxClientSoftware+1] is the whole name |
| `internal/protocol/conn.go:158:18` | > became >= | `if len(version) > MaxClientSoftware+1 {` | equivalent: at exactly MaxClientSoftware+1 bytes, version[:MaxClientSoftware+1] is the whole version |
| `internal/protocol/conn.go:185:16` | < became <= | `for i := 1; i < len(s)-1; i++ {` | equivalent: the loop reaching the last byte checks it for being a letter or digit, which the line above already required of it |
| `internal/protocol/frame.go:109:44` | < became <= | `if n > int64(limitFor(kmsg.Produce)) \|\| n < 2 {` | equivalent: a frame of exactly 2 bytes is only a key; read by the early path or the key path it is the same 2 bytes under a limit far above it |
| `internal/protocol/server.go:252:36` | > became >= | `} else if backoff *= 2; backoff > time.Second {` | equivalent: the backoff starts at 5ms and doubles (5, 10, ... 640, 1280 ms), so it never equals exactly one second and > and >= cap it alike |
| `internal/protocol/summary.go:698:14` | == became != | `if v.Kind() == reflect.Pointer && v.IsNil() {` | equivalent: firstError is handed a kmsg.Response, which is always a pointer; a typed nil pointer is answered 0 by walkErrors itself (its Pointer case), so this early return changes nothing |
| `internal/protocol/summary.go:469:24` | > became >= | `case len(req.Topics) > 1:` | equivalent: the case before it is len(req.Topics) == 1, so by the time this one is asked the length is not 1 and > 1 and >= 1 choose alike |
| `internal/protocol/summary.go:169:12` | < became <= | `if from < 0 {` | equivalent: from is read only when the request has exactly one partition, and with one partition it is assigned once, from -1 or from the first offset, whichever the test |
| `internal/protocol/summary.go:635:24` | < became <= | `case req.MemberEpoch < 0:` | equivalent: the case before it is MemberEpoch == 0, so by the time this one is asked the epoch is not 0 and < 0 and <= 0 choose alike |
| `internal/protocol/summary.go:263:24` | > became >= | `} else if len(stamps) > 1 {` | equivalent: the branch before it is len(stamps) == 1, so by the time this one is asked the length is not 1 and > 1 and >= 1 choose alike |
| `internal/protocol/versions.go:218:58` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })` | equivalent: the advertised APIs have distinct keys, so the sort never compares equals and < and <= order alike |

### Timed out (2)

| Where | Change | Line | Reason |
|---|---|---|---|
| `internal/protocol/summary.go:76:9` | + became - | `i = j + 1` | hang: i = j - 1 steps back over the run just written, so the partition-range loop never ends (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `internal/protocol/versions.go:191:40` | ++ became -- | `for m := firstMajor; m <= lastMajor; m++ {` | hang: the major-version loop counts down and never passes lastMajor (read from the code on 2026-10-16; the mutant itself was not run by hand) |

## internal/settings

### Not covered (1)

No test executes these lines. By declaration:

- `internal/settings/settings.go` `build`: 1 (lines 187)

## internal/storage/log

### Lived (92)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/storage/log/batch.go:154:20` | < became <= | `for pos := 0; pos < len(records); {` | equivalent: at pos == len(records) the slice is empty, parseHeader returns !ok and the loop returns, as it would have ended |
| `internal/storage/log/bounds.go:83:10` | < became <= | `if want < scanWindow {` | equivalent: at n == scanWindow both branches give want = scanWindow |
| `internal/storage/log/bounds.go:86:21` | > became >= | `if pos+int64(want) > r.size {` | equivalent: at pos+want == size the clamp size-pos equals want |
| `internal/storage/log/bounds.go:89:16` | < became <= | `if cap(r.buf) < want {` | equivalent: only decides reuse or reallocation of the read buffer; the bytes read are the same |
| `internal/storage/log/bounds.go:98:7` | < became <= | `if m < n {` | equivalent: at m == n, n = m changes nothing |
| `internal/storage/log/bounds.go:45:55` | > became >= | `if recorded := min(readLogStart(dir), end); recorded > start {` | equivalent: at equality start is assigned the value it already has |
| `internal/storage/log/cleanstop.go:137:32` | != became == | `if err := fsRemove(mark); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestBoundsAgreesWithRecoveryAfterEveryKindOfDamage (and 35 more) |
| `internal/storage/log/cleanstop.go:202:70` | == became != | `if bases, err := listSegmentBases(filepath.Join(m.dir, name)); err == nil && len(bases) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestEnsurePartitionDirsCreatesWithoutOpening, TestACleanStopDoesNotVouchForWhatItNeverOpened, TestRemoveStrayTopicDirsAndStrayDirs (and 6 more) |
| `internal/storage/log/cleanstop.go:56:46` | - became + | `names = make(map[string]struct{}, len(lines)-1)` | equivalent: only the capacity hint of a map |
| `internal/storage/log/compact.go:366:72` | - became + | `res := CleanResult{Ran: true, At: now, Complete: mapEnd == len(sealed)-1 \|\| endOf(mapEnd) >= activeBase}` | equivalent: for the last sealed segment the other disjunct already makes Complete true, and a segment's end cannot pass the active base |
| `internal/storage/log/compact.go:346:17` | < became <= | `for dirtyStart < len(elig) && elig[dirtyStart].baseOffset < cleanedTo {` | equivalent: dirty > 0 is checked earlier, so dirtyStart never reaches len(elig) |
| `internal/storage/log/compact.go:366:92` | >= became > | `res := CleanResult{Ran: true, At: now, Complete: mapEnd == len(sealed)-1 \|\| endOf(mapEnd) >= activeBase}` | equivalent: for the last sealed segment the other disjunct already makes Complete true, and a segment's end cannot pass the active base |
| `internal/storage/log/compact.go:175:91` | >= became > | `if n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil && n >= 0 {` | equivalent: the cleaned-to value read is 0 either way because cleanedTo starts at 0 |
| `internal/storage/log/compact.go:735:43` | >= became > | `if j := slices.Index(p.unsynced, old); j >= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads |
| `internal/storage/log/compact.go:651:60` | != became == | `if err := removeSegmentFiles(p.dir, old.baseOffset); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCompactionKeepsTheFirstSegmentAnchored, TestALogWithMoreKeysThanTheMapHoldsIsCleanedOverSeveralPasses, TestCompactionAndTransactions (and 5 more) |
| `internal/storage/log/compact.go:661:11` | ++ became -- | `p.dirSeq++` | *open:* no test asserts the count this statement keeps; reached by TestCompactionKeepsTheFirstSegmentAnchored, TestALogWithMoreKeysThanTheMapHoldsIsCleanedOverSeveralPasses, TestCompactionAndTransactions (and 5 more) |
| `internal/storage/log/compact.go:687:10` | ++ became -- | `p.dirSeq++` | *open:* no test asserts the count this statement keeps; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 17 more) |
| `internal/storage/log/compact.go:703:10` | ++ became -- | `p.dirSeq++` | *open:* no test asserts the count this statement keeps; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 17 more) |
| `internal/storage/log/compact.go:75:20` | < became <= | `for pos := 0; pos < len(records); {` | equivalent: at pos == len(records) parseHeader on empty input fails and returns, as the loop ending would |
| `internal/storage/log/compact.go:77:29` | <= became < | `if !ok \|\| info.totalBytes <= 0 \|\| pos+int(info.totalBytes) > len(records) {` | equivalent: parseHeader only succeeds with totalBytes >= 27, so totalBytes <= 0 is never true |
| `internal/storage/log/compact.go:226:26` | + became - | `for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {` | equivalent: the checks inside the loop (short head, !ok, ReadAt error) return on the same tails |
| `internal/storage/log/compact.go:787:26` | + became - | `for pos := int64(0); pos+batchFullHeaderBytes <= info.Size(); {` | equivalent: the checks inside the loop (short head, !ok, ReadAt error) return on the same tails |
| `internal/storage/log/compact.go:607:37` | * became / | `raw := make([]byte, 0, len(entries)*indexEntryBytes)` | equivalent: only the initial capacity of a slice that append grows |
| `internal/storage/log/compact.go:616:14` | > became >= | `if len(raw) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 21 more) |
| `internal/storage/log/compact.go:617:51` | != became == | `if _, err := (cleanedWriter{f}).Write(raw); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 16 more) |
| `internal/storage/log/compact.go:622:27` | != became == | `if err := fsSync(f); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 21 more) |
| `internal/storage/log/dirsync_unix.go:10:9` | != became == | `if err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads (and 138 more) |
| `internal/storage/log/dirsync_unix.go:13:26` | != became == | `if err := d.Sync(); err != nil {` | untestable: matters only when fsync of an opened directory fails, which cannot be provoked; on success the mutant returns the same nil |
| `internal/storage/log/durability.go:82:24` | >= became > | `if p.syncedSeq.Load() >= want {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads (and 135 more) |
| `internal/storage/log/durability.go:87:24` | >= became > | `if p.syncedSeq.Load() >= want {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestBoundsMatchAnOpenedPartition, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing (and 131 more) |
| `internal/storage/log/durability.go:100:45` | < became <= | `dirUpto, dirDirty := p.dirSeq, p.dirSynced < p.dirSeq` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestBoundsMatchAnOpenedPartition, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing (and 131 more) |
| `internal/storage/log/fs.go:140:9` | == became != | `if err == nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads (and 77 more) |
| `internal/storage/log/fs.go:194:33` | != became == | `if err := os.Remove(path); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads (and 74 more) |
| `internal/storage/log/fs.go:150:34` | != became == | `if err := f.Truncate(size); err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestEnsurePartitionDirsCreatesWithoutOpening, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads (and 84 more) |
| `internal/storage/log/index.go:56:59` | > became >= | `if _, err := ix.file.ReadAt(raw, 0); err != nil && count > 0 {` | equivalent: ReadAt of an empty buffer returns nil, so count >= 0 never adds an error |
| `internal/storage/log/index.go:107:37` | * became / | `raw := make([]byte, 0, len(entries)*indexEntryBytes)` | equivalent: only the initial capacity of a slice that append grows |
| `internal/storage/log/logstart.go:29:21` | < became <= | `if err != nil \|\| n < 0 {` | equivalent: n == 0 returns 0 either way |
| `internal/storage/log/manager.go:384:7` | > became >= | `if n > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestRemoveStrayTopicDirsAndStrayDirs, TestB_ManagerReportsADirectoryFlushOrRemovalThatFailed |
| `internal/storage/log/manager.go:398:29` | - became + | `if i <= 0 \|\| i == len(name)-1 {` | equivalent: for a name ending in '-', ParseInt of the empty rest fails and gives the same answer |
| `internal/storage/log/partition.go:249:17` | > became >= | `if len(done) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestFailedAppendDoesNotLeaveADuplicateOffset, TestAPayloadWhoseSecondBatchFailsLeavesNothing |
| `internal/storage/log/partition.go:263:20` | >= became > | `if len(d.batch) >= batchFullHeaderBytes && attrsOf(d.batch)&(batchTransactionalBit\|batchControlBit) != 0 {` | equivalent: a batch with a record is longer than the 61-byte header because checkBatchShape rejects a count of 0 |
| `internal/storage/log/partition.go:428:10` | ++ became -- | `p.dirSeq++` | *open:* no test asserts the count this statement keeps; reached by TestDeleteBeforeHidesWhatIsBelowAndFreesWholeSegments, TestDeleteBeforeSurvivesReopeningAndIsInTheClosedBounds, TestDeleteBeforeHighWatermarkEmptiesThePartitionOnDisk (and 10 more) |
| `internal/storage/log/partition.go:322:10` | < became <= | `for pos < seg.sizeBytes {` | equivalent: readAt at the end returns nothing, parseHeader fails, the loop breaks |
| `internal/storage/log/partition.go:336:30` | > became >= | `if limit >= 0 \|\| (maxBytes > 0 && maxBytes < n) {` | equivalent: at equality alignToBatch returns the whole n anyway |
| `internal/storage/log/partition.go:336:46` | < became <= | `if limit >= 0 \|\| (maxBytes > 0 && maxBytes < n) {` | equivalent: at equality alignToBatch returns the whole n anyway |
| `internal/storage/log/partition.go:165:17` | >= became > | `if p.dirSynced >= p.dirSeq {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing, TestCompactionKeepsTheLatestRecordPerKey (and 33 more) |
| `internal/storage/log/partition.go:363:9` | + became - | `for pos+total < seg.sizeBytes {` | equivalent: each batch is also guarded by pos+total+totalBytes > size, so the loop ends at the same batch |
| `internal/storage/log/partition.go:363:16` | < became <= | `for pos+total < seg.sizeBytes {` | equivalent: each batch is also guarded by pos+total+totalBytes > size, so the loop ends at the same batch |
| `internal/storage/log/produce.go:173:84` | + became - | `if ok && batch[batchMagicOff] == batchMagicV2 && len(batch) >= batchAttributesOff+2 && attrsOf(batch)&batchTransactionalBit != 0 {` | equivalent: firstBatch returns ok only for a batch of at least 26 bytes, so the length is never 23 |
| `internal/storage/log/produce.go:173:63` | >= became > | `if ok && batch[batchMagicOff] == batchMagicV2 && len(batch) >= batchAttributesOff+2 && attrsOf(batch)&batchTransactionalBit != 0 {` | equivalent: firstBatch returns ok only for a batch of at least 26 bytes, so the length is never 23 |
| `internal/storage/log/produce.go:490:9` | > became >= | `if n > 0 {` | equivalent: skip(0) does nothing |
| `internal/storage/log/produce.go:505:9` | > became >= | `if ts > scan.maxTimestamp {` | equivalent: an equal timestamp assigns the same value |
| `internal/storage/log/produce.go:460:13` | > became >= | `if keyLen > 0 {` | equivalent: skip(0) does nothing and a zero length cannot pass the bounds test |
| `internal/storage/log/produce.go:470:15` | > became >= | `if valueLen > 0 {` | equivalent: skip(0) does nothing and a zero length cannot pass the bounds test |
| `internal/storage/log/producerstate.go:41:32` | - became + | `first, size := len(p.segments)-1, int64(0)` | equivalent: the loop after it always assigns first because a partition has a segment |
| `internal/storage/log/producerstate.go:51:27` | + became - | `for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {` | equivalent: segment sizes end on batch boundaries (recovery truncates torn tails) and a short read breaks the loop at parseHeader anyway |
| `internal/storage/log/producerstate.go:57:53` | + became - | `if !ok \|\| len(head) < batchFullHeaderBytes \|\| pos+info.totalBytes > s.sizeBytes {` | equivalent: segment sizes end on batch boundaries (recovery truncates torn tails) and a short read breaks the loop at parseHeader anyway |
| `internal/storage/log/producerstate.go:61:85` | > became >= | `if ok && h.ProducerID >= 0 && !h.Control && h.BaseSequence >= 0 && h.RecordCount > 0 {` | equivalent: a stored batch never has a record count of 0 (checkBatchShape and scanLog reject count <= 0) |
| `internal/storage/log/records.go:207:19` | > became >= | `if int64(len(p)) > b.left+1 {` | equivalent: at equality the slice is the same |
| `internal/storage/log/records.go:312:8` | > became >= | `if n > 0 {` | equivalent: at n == 0 the reader returns (0, nil) and io.ReadAll/bufio retry it, so the bytes are the same |
| `internal/storage/log/records.go:145:41` | > became >= | `} else if rest := length - used; rest > 0 {` | equivalent: CopyN of 0 bytes is a no-op |
| `internal/storage/log/recovery.go:38:19` | < became <= | `if s.sizeBytes < before {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestBoundsMatchAnOpenedPartition, TestADamagedSealedSegmentIsFoundAndCutAndTheRestIsKept, TestAnUnvalidatedPartitionIsFlushedWholeBeforeItIsBelieved |
| `internal/storage/log/recovery.go:53:57` | > became >= | `if recorded := min(readLogStart(p.dir), leo); recorded > p.logStartOffset {` | equivalent: at equality the assignment writes the same value |
| `internal/storage/log/recovery.go:58:54` | <= became < | `for len(p.segments) > 1 && p.segments[1].baseOffset <= p.logStartOffset {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestBoundsMatchAnOpenedPartition, TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads, TestAPowerCutAtAnyStepOfACleaningPassLosesNothing (and 43 more) |
| `internal/storage/log/recovery.go:38:19` | < became >= | `if s.sizeBytes < before {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestBoundsMatchAnOpenedPartition, TestADamagedSealedSegmentIsFoundAndCutAndTheRestIsKept, TestAnUnvalidatedPartitionIsFlushedWholeBeforeItIsBelieved |
| `internal/storage/log/recovery.go:65:11` | ++ became -- | `p.dirSeq++` | *open:* no test asserts the count this statement keeps; reached by TestMutA_RemovedSegmentsLeaveNoIndexBehind |
| `internal/storage/log/recovery.go:143:10` | < became <= | `for pos < size {` | equivalent: at pos == size the next check, size-pos < 61, breaks at once |
| `internal/storage/log/retention.go:73:30` | <= became < | `if err != nil \|\| len(bases) <= 1 {` | equivalent: with one segment the removal loop (len(segs) > 1) does not run, so nothing changes |
| `internal/storage/log/segment.go:167:48` | * became / | `_ = fsTruncate(s.index.file, int64(m.entries)*indexEntryBytes)` | *open:* no test asserts the value this expression computes; reached by TestAPayloadWhoseSecondBatchFailsLeavesNothing |
| `internal/storage/log/segment.go:165:26` | > became >= | `if len(s.index.entries) > m.entries {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestAPayloadWhoseSecondBatchFailsLeavesNothing |
| `internal/storage/log/segment.go:165:26` | > became <= | `if len(s.index.entries) > m.entries {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAPayloadWhoseSecondBatchFailsLeavesNothing |
| `internal/storage/log/timestamps.go:191:37` | > became >= | `if i == first && p.logStartOffset > s.baseOffset {` | equivalent: when the log start equals the segment base nothing is hidden, so the walked maximum equals the header maximum |
| `internal/storage/log/timestamps.go:200:39` | > became >= | `if off >= p.logStartOffset && ts > m {` | equivalent: an equal value leaves the maximum unchanged |
| `internal/storage/log/timestamps.go:214:8` | > became >= | `if m > best {` | equivalent: an equal value leaves the maximum unchanged |
| `internal/storage/log/timestamps.go:48:17` | - became + | `if s.sizeBytes-pos < batchFullHeaderBytes {` | equivalent: segment sizes end on batch boundaries, a short read fails parseHeader and breaks the loop |
| `internal/storage/log/timestamps.go:56:16` | + became - | `if !ok \|\| pos+info.totalBytes > s.sizeBytes {` | equivalent: segment sizes end on batch boundaries, a short read fails parseHeader and breaks the loop |
| `internal/storage/log/timestamps.go:46:19` | < became <= | `for s.ts.scanned < s.sizeBytes {` | equivalent: the extra iteration breaks at once on the short-header check |
| `internal/storage/log/timestamps.go:63:76` | > became >= | `if m := int64(binary.BigEndian.Uint64(header[batchMaxTimestampOff:])); m > s.ts.max {` | equivalent: an equal value leaves the maximum unchanged |
| `internal/storage/log/timestamps.go:105:16` | + became - | `if !ok \|\| pos+info.totalBytes > s.ts.scanned {` | equivalent: segment sizes end on batch boundaries, a short read fails parseHeader |
| `internal/storage/log/timestamps.go:99:23` | < became <= | `for pos := from; pos < s.ts.scanned; {` | equivalent: readAt past EOF returns nothing, parseHeader fails and the walk returns nil |
| `internal/storage/log/txn.go:262:16` | + became - | `if !ok \|\| pos+int(info.totalBytes) > len(data) {` | equivalent: data holds only whole batches (alignToBatch), so the condition is never true |
| `internal/storage/log/txn.go:260:20` | < became <= | `for pos := 0; pos < len(data); {` | equivalent: at pos == len(data) the header parse of empty input fails and the loop breaks |
| `internal/storage/log/txn.go:235:66` | < became <= | `sort.Slice(out, func(i, j int) bool { return out[i].FirstOffset < out[j].FirstOffset })` | equivalent: first offsets are distinct, so the sort has no ties |
| `internal/storage/log/txn.go:116:28` | + became - | `for pos := int64(0); pos+batchFullHeaderBytes <= s.sizeBytes; {` | equivalent: segment sizes end on batch boundaries; a short read breaks at parseHeader |
| `internal/storage/log/txn.go:124:18` | + became - | `if !ok \|\| pos+info.totalBytes > s.sizeBytes \|\| len(head) < batchFullHeaderBytes {` | equivalent: segment sizes end on batch boundaries; a short read breaks at parseHeader |
| `internal/storage/log/txn.go:127:63` | < became <= | `if attrsOf(head)&batchControlBit != 0 && int64(len(head)) < info.totalBytes {` | equivalent: the control key lies within the first 32 bytes after the header, so re-reading a longer control batch gives the same answer |
| `internal/storage/log/txn.go:127:38` | != became == | `if attrsOf(head)&batchControlBit != 0 && int64(len(head)) < info.totalBytes {` | equivalent: the control key lies within the first 32 bytes after the header, so re-reading a longer control batch gives the same answer |
| `internal/storage/log/txn.go:127:63` | < became >= | `if attrsOf(head)&batchControlBit != 0 && int64(len(head)) < info.totalBytes {` | equivalent: the control key lies within the first 32 bytes after the header, so re-reading a longer control batch gives the same answer |
| `internal/storage/log/txn.go:61:7` | <= became < | `if n <= 0 \|\| len(body) < n+1 {` | equivalent: the boundary case is an empty body and the next check returns false either way |
| `internal/storage/log/txn.go:61:25` | < became <= | `if n <= 0 \|\| len(body) < n+1 {` | equivalent: the boundary case is an empty body and the next check returns false either way |
| `internal/storage/log/txn.go:67:8` | <= became < | `if m <= 0 {` | equivalent: the boundary case is an empty body and the next check returns false either way |
| `internal/storage/log/txn.go:73:7` | <= became < | `if m <= 0 \|\| keyLen < 4 \|\| int64(len(body)-m) < keyLen {` | equivalent: the boundary case is an empty body and the next check returns false either way |

### Timed out (13)

| Where | Change | Line | Reason |
|---|---|---|---|
| `internal/storage/log/manager.go:309:38` | ++ became -- | `for p := int32(0); p < partitions; p++ {` | open |
| `internal/storage/log/manager.go:464:38` | ++ became -- | `for p := int32(0); p < partitions; p++ {` | open |
| `internal/storage/log/partition.go:331:46` | <= became < | `if info.baseOffset+int64(info.recordCount) <= from {` | equivalent: batches wholly below from are skipped by the caller's own bound as well |
| `internal/storage/log/partition.go:352:8` | == became != | `if s == seg && i+1 < len(p.segments) {` | open |
| `internal/storage/log/produce.go:389:8` | > became >= | `for n > 0 && r.err == nil {` | open |
| `internal/storage/log/produce.go:394:10` | != became == | `if err != nil {` | open |
| `internal/storage/log/produce.go:555:28` | ++ became -- | `for i := 1; i < len(rs); i++ {` | open |
| `internal/storage/log/records.go:208:16` | + became - | `p = p[:b.left+1] // one past the limit is enough to know it was passed.` | open |
| `internal/storage/log/records.go:217:12` | < became <= | `if b.left < 0 {` | killed by hang: bounded test TestF_* fails in 5 s; the run times out because other tests reaching the line hang |
| `internal/storage/log/records.go:256:17` | == became != | `for len(x.cur) == 0 {` | open |
| `internal/storage/log/records.go:268:10` | != became == | `if err != nil {` | open |
| `internal/storage/log/timestamps.go:89:18` | + became - | `if p.segments[i+1].baseOffset > p.logStartOffset {` | open |
| `internal/storage/log/timestamps.go:88:38` | ++ became -- | `for i := 0; i+1 < len(p.segments); i++ {` | open |

### Not covered (5)

No test executes these lines. By declaration:

- `internal/storage/log/compact.go` `(*Partition).swapSegment`: 2 (lines 652)
- `internal/storage/log/fs.go` `fsMkdir`: 1 (lines 216)
- `internal/storage/log/manager.go` `(*Manager).maintain`: 1 (lines 673)
- `internal/storage/log/partition.go` `alignToBatch`: 1 (lines 381)

## internal/storage/meta

### Lived (15)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/storage/meta/dirsync_unix.go:11:9` | != became == | `if err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestACLsStoreListRemoveAndPersist, TestOffsetsRecordWhenTheyWereCommitted, TestExpireOffsets (and 29 more) |
| `internal/storage/meta/dirsync_unix.go:14:26` | != became == | `if err := d.Sync(); err != nil {` | untestable: matters only when fsync of an opened directory fails, which cannot be provoked; on success the mutant returns the same nil |
| `internal/storage/meta/store.go:1002:9` | < became <= | `if i < 0 \|\| bytes.Equal(k[:i], last) {` | equivalent while AdoptGroupsWithOffsets skips the empty group id through bytes.Equal(k[:0], nil) (a bug, reported, not fixed here) |
| `internal/storage/meta/store.go:780:7` | < became <= | `if n < 1 {` | equivalent: with n == 1 the mutant sets n = 1, the value it already has |
| `internal/storage/meta/store.go:939:37` | + became - | `if len(rest) < 2 \|\| len(rest) < 2+int(binary.BigEndian.Uint16(rest)) {` | *open:* no test asserts the value this expression computes; reached by TestGroupLivenessSurvivesAReopen, TestAdoptGroupsWithOffsetsRunsOnce |
| `internal/storage/meta/store.go:943:28` | + became - | `out := string(rest[2 : 2+n])` | *open:* no test asserts the value this expression computes; reached by TestGroupLivenessSurvivesAReopen, TestAdoptGroupsWithOffsetsRunsOnce |
| `internal/storage/meta/store.go:944:17` | + became - | `rest = rest[2+n:]` | *open:* no test asserts the value this expression computes; reached by TestGroupLivenessSurvivesAReopen, TestAdoptGroupsWithOffsetsRunsOnce |
| `internal/storage/meta/store.go:931:14` | >= became > | `case len(v) >= 24 && v[0] == livenessV2:` | *open:* no test puts the two sides equal and asserts which way the comparison goes |
| `internal/storage/meta/store.go:939:17` | < became <= | `if len(rest) < 2 \|\| len(rest) < 2+int(binary.BigEndian.Uint16(rest)) {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestGroupLivenessSurvivesAReopen, TestAdoptGroupsWithOffsetsRunsOnce |
| `internal/storage/meta/store.go:939:34` | < became <= | `if len(rest) < 2 \|\| len(rest) < 2+int(binary.BigEndian.Uint16(rest)) {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestGroupLivenessSurvivesAReopen, TestAdoptGroupsWithOffsetsRunsOnce |
| `internal/storage/meta/store.go:928:14` | == became != | `case len(v) == 9 && (v[0] == liveClassic \|\| v[0] == liveConsumer):` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `internal/storage/meta/store.go:928:28` | == became != | `case len(v) == 9 && (v[0] == liveClassic \|\| v[0] == liveConsumer):` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `internal/storage/meta/store.go:928:51` | == became != | `case len(v) == 9 && (v[0] == liveClassic \|\| v[0] == liveConsumer):` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `internal/storage/meta/store.go:423:48` | != became == | `if h := durabilityHook.Load(); h != nil && *h != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestACLsStoreListRemoveAndPersist, TestOffsetsRecordWhenTheyWereCommitted, TestExpireOffsets (and 29 more) |
| `internal/storage/meta/store.go:636:12` | < became <= | `if len(k) < 6 {` | equivalent: a 6-byte key is two NULs and a partition, whose topic is the empty string either way |

### Timed out (2)

| Where | Change | Line | Reason |
|---|---|---|---|
| `internal/storage/meta/store.go:607:28` | != became == | `for k, _ := c.First(); k != nil; k, _ = c.Next() {` | hang or nothing: the bucket walk stops at once or runs past its end; the reopen tests wait (read from the code on 2026-10-16; the mutant itself was not run by hand) |
| `internal/storage/meta/store.go:85:37` | != became == | `for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {` | hang: the case-fold orbit loop runs while f == r, which never changes (read from the code on 2026-10-16; the mutant itself was not run by hand) |

### Not covered (2)

No test executes these lines. By declaration:

- `internal/storage/meta/store.go` `decodeGroupLiveness`: 2 (lines 930, 929)

## internal/telemetry

### Lived (8)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/telemetry/otlp.go:92:8` | < became <= | `if n < 0 {` | equivalent: protowire.ConsumeTag returns a positive length or a negative error code, never 0, so n < 0 and n <= 0 choose alike (golang protobuf protowire documents n<0 as the error; read from the code, mutant not run by hand) |
| `internal/telemetry/otlp.go:103:9` | < became <= | `if m < 0 {` | equivalent: protowire.ConsumeVarint returns a positive length or a negative error code, never 0, so m < 0 and m <= 0 choose alike |
| `internal/telemetry/otlp.go:109:9` | < became <= | `if m < 0 {` | equivalent: protowire.ConsumeFixed64 returns 8 or a negative error code, never 0, so m < 0 and m <= 0 choose alike |
| `internal/telemetry/otlp.go:115:9` | < became <= | `if m < 0 {` | equivalent: protowire.ConsumeFixed32 returns 4 or a negative error code, never 0, so m < 0 and m <= 0 choose alike |
| `internal/telemetry/otlp.go:121:9` | < became <= | `if m < 0 {` | equivalent: protowire.ConsumeBytes returns at least 1 (the length prefix) or a negative error code, never 0, so m < 0 and m <= 0 choose alike |
| `internal/telemetry/store.go:500:30` | < became <= | `return cp.Series[a].Name < cp.Series[b].Name` | equivalent: the comparator is reached only when the two names differ (line 499), so Name < and Name <= answer alike |
| `internal/telemetry/store.go:502:30` | < became <= | `return cp.Series[a].Attrs < cp.Series[b].Attrs` | equivalent: a series is keyed by name and attributes, so no two series of one instance have both equal, and the comparator never sees a tie |
| `internal/telemetry/store.go:136:24` | > became >= | `if n := len(s.off); n > samplesKept {` | equivalent: at exactly samplesKept points drop is 0 and shift is s.off[0], which is always 0 (base is the first kept sample's time), so the trim at == changes nothing |

## internal/traffic

### Lived (10)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `internal/traffic/story.go:70:21` | <= became < | `if err != nil \|\| n <= 0 {` | equivalent: n <= 0 and n < 0 differ only at n == 0, where the first returns 0 and the second returns int32(0), which is 0 (read from the code) |
| `internal/traffic/traffic.go:1088:31` | < became <= | `if oldest == "" \|\| l.last < at {` | equivalent: each group's last is the recorder's sequence number at its latest event, and sequence numbers are unique, so two groups never tie and < and <= pick alike |
| `internal/traffic/traffic.go:1103:19` | > became >= | `if len(g.events) > MaxGroupEvents {` | equivalent: at exactly MaxGroupEvents the trim is slices.Delete(events, 0, 0), which removes nothing |
| `internal/traffic/traffic.go:1117:27` | > became >= | `if limit > 0 && len(out) > limit {` | equivalent: at exactly limit the cut is out[len-limit:] = out[0:], the whole list |
| `internal/traffic/traffic.go:741:13` | > became >= | `if q.Limit > 0 {` | equivalent: with Limit 0 the list is made with capacity min(0, n) = 0, and the appends that follow give the same rows as the unallocated one |
| `internal/traffic/traffic.go:764:14` | > became >= | `if q.Limit > 0 {` | equivalent: with Limit 0 the insertion pass reorders a list that the full sort below (q.Limit <= 0) puts in the same final order |
| `internal/traffic/traffic.go:509:21` | > became >= | `if len(o.TopicIDs) > 0 && r.resolve != nil {` | equivalent: with no topic ids the loop adds nothing, and the clone of o.Topics holds the same names |
| `internal/traffic/traffic.go:319:56` | > became >= | `if b := c.b[i]; b.idx >= lo && b.idx < hi && b.total > 0 {` | equivalent: a bucket with total 0 adds 0 to both sums |
| `internal/traffic/traffic.go:390:14` | < became <= | `if capacity < 1 {` | equivalent: capacity 1 is set to 1 |
| `internal/traffic/traffic.go:724:15` | > became >= | `return a.Seq > b.Seq` | equivalent: rows never share a sequence number (each fold and each new row takes the next), so > and >= answer alike for two different rows; newer is not asked of a row against itself |

### Timed out (1)

| Where | Change | Line | Reason |
|---|---|---|---|
| `internal/traffic/traffic.go:451:19` | == became != | `} else if r.head == x {` | hang: unlinking the head leaves it in place, and the eviction loop that unlinks the oldest row never shrinks the list (read from the code on 2026-10-16; the mutant itself was not run by hand) |

## schemaregistry

### Lived (102)

| Where | Change | Line | What lets it live |
|---|---|---|---|
| `schemaregistry/api.go:329:44` | > became >= | `if boolParam(r, "verbose") && len(found) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance, TestCorpus, FuzzAPIBody (and 1 more) |
| `schemaregistry/avro.go:161:20` | > became >= | `if len(ty.items) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avro.go:172:60` | > became >= | `if sy := ty.get("symbols"); sy != nil && len(sy.items) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus |
| `schemaregistry/avro.go:161:20` | > became <= | `if len(ty.items) > 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avro.go:177:33` | != became == | `if sz := ty.get("size"); sz != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus |
| `schemaregistry/avro.go:101:12` | != became == | `if ty != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avro.go:116:44` | != became == | `if v := n.get(key); v != nil && v.kind != 's' {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avroguard.go:87:8` | < became <= | `if n < 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes (and 4 more) |
| `schemaregistry/avroguard.go:98:12` | < became <= | `if size < 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes (and 4 more) |
| `schemaregistry/avroguard.go:57:18` | > became >= | `if i == 9 && c > 1 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes (and 6 more) |
| `schemaregistry/avroguard.go:57:18` | > became <= | `if i == 9 && c > 1 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes (and 6 more) |
| `schemaregistry/avroguard.go:149:73` | + became - | `return w.blocks("an array", func() error { return w.value(items, depth+1) })` | *open:* no test asserts the value this expression computes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes (and 4 more) |
| `schemaregistry/avroguard.go:160:32` | + became - | `return w.value(values, depth+1)` | *open:* no test asserts the value this expression computes; reached by TestTheGuardLetsThroughWhatAnEncoderWrites, TestTheGuardAcceptsEverythingTheLibraryEncodes, TestRandomBytesNeverCostMoreThanTheyWeigh (and 1 more) |
| `schemaregistry/avrotext.go:346:29` | != became == | `if space != nil && *space != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 16 more) |
| `schemaregistry/avrotext.go:270:69` | < became <= | `sort.SliceStable(idx, func(a, b int) bool { return n.keys[idx[a]] < n.keys[idx[b]] })` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestAvroRoundTrip, TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avrotext.go:270:69` | < became >= | `sort.SliceStable(idx, func(a, b int) bool { return n.keys[idx[a]] < n.keys[idx[b]] })` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestAvroRoundTrip, TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/avrotext.go:307:42` | >= became > | `if i := strings.LastIndex(name, "."); i >= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 6 more) |
| `schemaregistry/avrotext.go:312:43` | != became == | `} else if enclosing != nil && *enclosing != "" && !w.defined[name] {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 4 more) |
| `schemaregistry/avrotext.go:315:11` | == became != | `if space == nil \|\| nsEqual(space, enclosing) \|\| *space == "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 6 more) |
| `schemaregistry/avrotext.go:315:57` | == became != | `if space == nil \|\| nsEqual(space, enclosing) \|\| *space == "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 6 more) |
| `schemaregistry/avrotext.go:166:9` | != became == | `if err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:179:9` | == became != | `if t == nil \|\| t.kind != 's' {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:179:26` | != became == | `if t == nil \|\| t.kind != 's' {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:189:14` | != became == | `if space != nil && *space != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:189:31` | != became == | `if space != nil && *space != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:193:32` | != became == | `if f := n.get("fields"); f != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:195:36` | != became == | `if ft := fld.get("type"); ft != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/avrotext.go:235:21` | == became != | `case a == nil && b == nil:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/avrotext.go:222:42` | >= became > | `if i := strings.LastIndex(name, "."); i >= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestHostileAvroIsRefusedBeforeTheDecoderSeesIt, TestTheGuardLetsThroughWhatAnEncoderWrites, TestRandomBytesNeverCostMoreThanTheyWeigh (and 16 more) |
| `schemaregistry/codec.go:281:75` | == became != | `if rv.IsValid() && rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestTheGuardLetsThroughWhatAnEncoderWrites, TestDecodesWhatConfluentsSerializersWrite, TestAvroRoundTrip (and 1 more) |
| `schemaregistry/codec.go:224:22` | - became + | `if depth < len(idx)-1 {` | *open:* no test asserts the value this expression computes; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode (and 1 more) |
| `schemaregistry/codec.go:220:17` | >= became > | `if i < 0 \|\| i >= msgs.Len() {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode (and 1 more) |
| `schemaregistry/codec.go:224:12` | < became <= | `if depth < len(idx)-1 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode (and 1 more) |
| `schemaregistry/codec.go:224:12` | < became >= | `if depth < len(idx)-1 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode (and 1 more) |
| `schemaregistry/codec.go:511:9` | < became <= | `if i < math.MinInt32 \|\| i > math.MaxInt32 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestTheGuardLetsThroughWhatAnEncoderWrites, TestAvroRoundTrip |
| `schemaregistry/codec.go:511:30` | > became >= | `if i < math.MinInt32 \|\| i > math.MaxInt32 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestTheGuardLetsThroughWhatAnEncoderWrites, TestAvroRoundTrip |
| `schemaregistry/codec.go:196:7` | <= became < | `if k <= 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode (and 1 more) |
| `schemaregistry/codec.go:203:7` | < became <= | `if n < 0 \|\| n > 64 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode |
| `schemaregistry/codec.go:203:16` | > became >= | `if n < 0 \|\| n > 64 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode |
| `schemaregistry/codec.go:208:8` | <= became < | `if k <= 0 \|\| v < 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode |
| `schemaregistry/codec.go:208:18` | < became <= | `if k <= 0 \|\| v < 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestProtobufRoundTripAndNestedMessageIndex, FuzzDecode |
| `schemaregistry/codec.go:302:8` | > became >= | `if r > 255 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestTheGuardLetsThroughWhatAnEncoderWrites, TestAvroRoundTrip |
| `schemaregistry/jsoncompat.go:815:24` | > became >= | `if len(sub.changes) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:806:34` | == became != | `case o.kind == jFalse && u.kind == jFalse:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:815:24` | > became <= | `if len(sub.changes) > 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:747:19` | > became >= | `if len(u.tuple) > count {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:762:17` | < became <= | `for i := 0; i < count; i++ {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:794:25` | != became == | `case o.additionalItem != nil && u.additionalItem == nil:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:794:52` | == became != | `case o.additionalItem != nil && u.additionalItem == nil:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:796:25` | != became == | `case o.additionalItem != nil && u.additionalItem != nil:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:796:52` | != became == | `case o.additionalItem != nil && u.additionalItem != nil:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:738:24` | != became == | `if n.additionalItem != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:741:15` | == became != | `if n.tuple == nil && n.items != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:741:33` | != became == | `if n.tuple == nil && n.items != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:1021:19` | > became >= | `if unmatchedOld > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:1032:18` | > became >= | `if unmatchedNew > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:1021:19` | > became <= | `if unmatchedOld > 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:1032:18` | > became <= | `if unmatchedNew > 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:968:21` | == became != | `case oc.criterion == "anyOf" && uc.criterion == "oneOf":` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:968:48` | == became != | `case oc.criterion == "anyOf" && uc.criterion == "oneOf":` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:971:21` | == became != | `case len(oc.subs) == 1:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:973:21` | == became != | `case len(uc.subs) == 1:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:542:34` | == became != | `case o.kind == jFalse && u.kind == jFalse:` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/jsoncompat.go:911:24` | == became != | `if o.additional.kind == jFalse {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:905:24` | == became != | `if u.additional.kind == jFalse {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:118:13` | > became >= | `if l.depth > 64 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestDecodesWhatConfluentsSerializersWrite, TestJSONSchemaEncodeValidates, TestConformance (and 7 more) |
| `schemaregistry/jsoncompat.go:116:9` | ++ became -- | `l.depth++` | *open:* no test asserts the count this statement keeps; reached by TestDecodesWhatConfluentsSerializersWrite, TestJSONSchemaEncodeValidates, TestConformance (and 7 more) |
| `schemaregistry/jsoncompat.go:117:24` | -- became ++ | `defer func() { l.depth-- }()` | *open:* no test asserts the count this statement keeps; reached by TestDecodesWhatConfluentsSerializersWrite, TestJSONSchemaEncodeValidates, TestConformance (and 7 more) |
| `schemaregistry/jsoncompat.go:213:36` | != became == | `if n, ok := l.memo[ref]; ok && n != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, TestReferencesInJSONAndProtobuf, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:195:9` | != became == | `if err != nil {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/jsoncompat.go:186:16` | == became != | `return f, err == nil` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/protocompat.go:283:46` | != became == | `if ol != "" && ul != "" && ol != ul && !(ol != "required" && ul != "required" && lengthDelimited(o) && lengthDelimited(u)) {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, TestReferencesInJSONAndProtobuf, FuzzDescriptor (and 1 more) |
| `schemaregistry/protocompat.go:283:66` | != became == | `if ol != "" && ul != "" && ol != ul && !(ol != "required" && ul != "required" && lengthDelimited(o) && lengthDelimited(u)) {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, TestReferencesInJSONAndProtobuf, FuzzDescriptor (and 1 more) |
| `schemaregistry/protocompat.go:304:10` | == became != | `case og == "message" \|\| ug == "message":` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/protocompat.go:304:29` | == became != | `case og == "message" \|\| ug == "message":` | *open:* the tests that reach the line assert nothing that depends on which branch is taken |
| `schemaregistry/protocompat.go:265:36` | != became == | `if p := string(file.Package()); p != "" && m.ParentFile() == file {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/protocompat.go:265:60` | == became != | `if p := string(file.Package()); p != "" && m.ParentFile() == file {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, FuzzRegisterAndCompare |
| `schemaregistry/protocompat.go:114:14` | != became == | `if prefix != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestCorpus, TestReferencesInJSONAndProtobuf, FuzzDescriptor (and 1 more) |
| `schemaregistry/protofromdesc.go:247:58` | == became != | `return len(m.GetField()) == 0 && len(m.GetNestedType()) == 0 && len(m.GetEnumType()) == 0 && len(m.GetReservedRange()) == 0 &&` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:247:87` | == became != | `return len(m.GetField()) == 0 && len(m.GetNestedType()) == 0 && len(m.GetEnumType()) == 0 && len(m.GetReservedRange()) == 0 &&` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:247:121` | == became != | `return len(m.GetField()) == 0 && len(m.GetNestedType()) == 0 && len(m.GetEnumType()) == 0 && len(m.GetReservedRange()) == 0 &&` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:248:28` | == became != | `len(m.GetReservedName()) == 0 && len(m.GetOneofDecl()) == 0 && !m.GetOptions().GetMapEntry()` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:248:58` | == became != | `len(m.GetReservedName()) == 0 && len(m.GetOneofDecl()) == 0 && !m.GetOptions().GetMapEntry()` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:44:66` | == became != | `if err := proto.Unmarshal(raw, &fd); err != nil \|\| fd.GetName() == "" && len(fd.GetMessageType()) == 0 && len(fd.GetEnumType()) == 0 && len(fd.GetService()) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, TestNotAProtobufSchemaAtAll, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:44:100` | == became != | `if err := proto.Unmarshal(raw, &fd); err != nil \|\| fd.GetName() == "" && len(fd.GetMessageType()) == 0 && len(fd.GetEnumType()) == 0 && len(fd.GetService()) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, TestNotAProtobufSchemaAtAll, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:44:130` | == became != | `if err := proto.Unmarshal(raw, &fd); err != nil \|\| fd.GetName() == "" && len(fd.GetMessageType()) == 0 && len(fd.GetEnumType()) == 0 && len(fd.GetService()) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, TestNotAProtobufSchemaAtAll, FuzzDescriptor |
| `schemaregistry/protofromdesc.go:44:159` | == became != | `if err := proto.Unmarshal(raw, &fd); err != nil \|\| fd.GetName() == "" && len(fd.GetMessageType()) == 0 && len(fd.GetEnumType()) == 0 && len(fd.GetService()) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestADescriptorIsStoredAsConfluentStoresIt, TestNotAProtobufSchemaAtAll, FuzzDescriptor |
| `schemaregistry/protoprint.go:108:30` | > became >= | `if len(messages)+len(enums) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestDecodesWhatConfluentsSerializersWrite, TestProtobufRoundTripAndNestedMessageIndex, TestConformance (and 8 more) |
| `schemaregistry/registry.go:641:30` | > became >= | `if latestOnly && len(keep) > 0 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance |
| `schemaregistry/registry.go:650:26` | < became <= | `return out[i].Subject < out[j].Subject` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance |
| `schemaregistry/registry.go:652:25` | < became <= | `return out[i].Version < out[j].Version` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance |
| `schemaregistry/registry.go:867:86` | > became >= | `if level != CompatNone && !strings.HasSuffix(level, "_TRANSITIVE") && len(against) > 1 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance, TestCompatibilityLevelsChooseTheVersionsCompared |
| `schemaregistry/registry.go:1008:47` | != became == | `if _, ok := r.modes[subject]; !ok && subject != "" {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/registry.go:619:26` | < became <= | `return out[i].Subject < out[j].Subject` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance |
| `schemaregistry/registry.go:618:21` | != became == | `if out[i].Subject != out[j].Subject {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance |
| `schemaregistry/registry.go:239:38` | + became - | `if err := walk(s.References, depth+1); err != nil {` | *open:* no test asserts the value this expression computes; reached by TestConformance, TestReferencesInJSONAndProtobuf, FuzzAPIBody |
| `schemaregistry/registry.go:230:12` | > became >= | `if depth > 32 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance, TestCorpus, TestARequestForAForeignHostIsRefusedAndChangesNothing (and 12 more) |
| `schemaregistry/registry.go:118:61` | < became <= | `sort.Slice(vs, func(i, j int) bool { return vs[i].Version < vs[j].Version })` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestEverythingSurvivesARestart |
| `schemaregistry/registry.go:121:9` | >= became > | `if id >= r.nextID {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestEverythingSurvivesARestart |
| `schemaregistry/registry.go:118:61` | < became >= | `sort.Slice(vs, func(i, j int) bool { return vs[i].Version < vs[j].Version })` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestEverythingSurvivesARestart |
| `schemaregistry/registry.go:513:30` | > became >= | `if err != nil \|\| n < 1 \|\| n > math.MaxInt32 {` | *open:* no test puts the two sides equal and asserts which way the comparison goes; reached by TestConformance, TestARequestForAForeignHostIsRefusedAndChangesNothing, TestEverythingSurvivesARestart (and 2 more) |
| `schemaregistry/registry.go:292:15` | == became != | `if len(refs) == 0 {` | *open:* the tests that reach the line assert nothing that depends on which branch is taken; reached by TestConformance, TestCorpus, FuzzDescriptor (and 1 more) |

### Not covered (36)

No test executes these lines. By declaration:

- `schemaregistry/avro.go` `parseAvro`: 1 (lines 45)
- `schemaregistry/avrotext.go` `(*avroWriter).reference`: 2 (lines 319)
- `schemaregistry/avrotext.go` `avroDefinedNames`: 3 (lines 205, 180, 201)
- `schemaregistry/codec.go` `primitiveToAvro`: 4 (lines 504, 506, 508, 439)
- `schemaregistry/codec.go` `stringToBytes`: 1 (lines 305)
- `schemaregistry/jsoncompat.go` `(*jdiff).array`: 4 (lines 797, 752, 772, 782)
- `schemaregistry/jsoncompat.go` `(*jdiff).compare`: 4 (lines 583)
- `schemaregistry/jsoncompat.go` `(*jdiff).object`: 2 (lines 848)
- `schemaregistry/jsoncompat.go` `(*jsonLoader).ofType`: 2 (lines 369, 377)
- `schemaregistry/jsoncompat.go` `equalStrings`: 1 (lines 946)
- `schemaregistry/jsoncompat.go` `intPtr`: 1 (lines 197)
- `schemaregistry/protocompat.go` `(*protoDiff).typeName`: 1 (lines 266)
- `schemaregistry/protofromdesc.go` `printDescriptor`: 2 (lines 71, 76)
- `schemaregistry/protoprint.go` `(*protoPrinter).enum`: 1 (lines 279)
- `schemaregistry/protoprint.go` `(*protoPrinter).extend`: 1 (lines 294)
- `schemaregistry/protoprint.go` `(*protoPrinter).messageBody`: 1 (lines 233)
- `schemaregistry/registry.go` `(*Registry).IDSubjects`: 2 (lines 621)
- `schemaregistry/registry.go` `Open`: 1 (lines 122)
- `schemaregistry/registry.go` `refsString`: 2 (lines 299)

