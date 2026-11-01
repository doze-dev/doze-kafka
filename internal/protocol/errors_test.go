package protocol

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A classic consumer's first JoinGroup is answered MEMBER_ID_REQUIRED on purpose, and a
// heartbeat that tells a member to rejoin is REBALANCE_IN_PROGRESS: both are listed as
// errors today, which put an "error" on every consumer's start and made "errors only"
// and the error count lie (TRF-8).
func TestExpectedFlowIsNotAnError(t *testing.T) {
	for _, c := range []struct {
		name string
		api  APIKey
		code int16
		want Class
	}{
		{"a first join", kmsg.JoinGroup, kerr.MemberIDRequired.Code, ClassExpected},
		{"a heartbeat during a rebalance", kmsg.Heartbeat, kerr.RebalanceInProgress.Code, ClassExpected},
		{"a sync during a rebalance", kmsg.SyncGroup, kerr.RebalanceInProgress.Code, ClassExpected},
		{"version negotiation", kmsg.ApiVersions, kerr.UnsupportedVersion.Code, ClassExpected},
		{"nothing wrong", kmsg.Produce, 0, ClassOK},
		{"a topic that does not exist", kmsg.Produce, kerr.UnknownTopicOrPartition.Code, ClassRefused},
		{"a fenced producer", kmsg.Produce, kerr.ProducerFenced.Code, ClassRefused},
		{"a group the principal may not read", kmsg.OffsetFetch, kerr.GroupAuthorizationFailed.Code, ClassDenied},
		{"a bad login", kmsg.SASLAuthenticate, kerr.SaslAuthenticationFailed.Code, ClassDenied},
		{"a version the broker does not offer", kmsg.Fetch, kerr.UnsupportedVersion.Code, ClassNotImplemented},
		{"a full disk", kmsg.Produce, kerr.KafkaStorageError.Code, ClassFailed},
		{"a bug", kmsg.Fetch, kerr.UnknownServerError.Code, ClassFailed},
		{"a code with no row that Kafka retries", kmsg.Fetch, kerr.RequestTimedOut.Code, ClassFailed},
		{"a code with no row that it does not", kmsg.Fetch, kerr.InvalidTimestamp.Code, ClassRefused},
	} {
		if got := Classify(c.api, c.code); got != c.want {
			t.Errorf("%s: %s %d is %q, want %q", c.name, APIName(c.api), c.code, got, c.want)
		}
	}
	for class, want := range map[Class]bool{ClassOK: false, ClassExpected: false, ClassRefused: true, ClassDenied: true, ClassNotImplemented: true, ClassFailed: true} {
		if got := class.IsError(); got != want {
			t.Errorf("%q.IsError() = %v, want %v", class, got, want)
		}
	}
}

// The describing observer says the class, and an expected answer carries its code
// without being an error.
func TestDescribeCarriesTheClass(t *testing.T) {
	req := &Request{Header: RequestHeader{APIKey: kmsg.JoinGroup}}
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.ErrorCode = kerr.MemberIDRequired.Code
	req.answered = resp
	s := req.Describe()
	if s.ErrorCode != kerr.MemberIDRequired.Code || s.Class != ClassExpected {
		t.Errorf("describe = code %d class %q, want MEMBER_ID_REQUIRED and expected", s.ErrorCode, s.Class)
	}
	resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
	if s := req.Describe(); s.Class != ClassDenied {
		t.Errorf("class = %q, want denied", s.Class)
	}
}

// Every error the broker's own code returns has a row: its class, its usual cause and
// its fix. The page explains what it can name, so a code that appears there without a row
// would be an unexplained red word.
func TestEveryErrorTheBrokerReturnsHasARow(t *testing.T) {
	byName := map[string]*kerr.Error{}
	for c := int16(-1); c < 200; c++ {
		if e := kerr.TypedErrorForCode(c); e != nil && e.Code == c {
			byName[strings.ToLower(strings.ReplaceAll(e.Message, "_", ""))] = e
		}
	}
	files, err := filepath.Glob("../broker/*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no broker sources: %v", err)
	}
	used := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "kerr" {
				if e := byName[strings.ToLower(sel.Sel.Name)]; e != nil {
					used[e.Message] = true
				}
			}
			return true
		})
	}
	if len(used) < 40 {
		t.Fatalf("found only %d error codes in internal/broker: the walk is not finding them", len(used))
	}
	for name := range used {
		var code int16
		for c := int16(-1); c < 200; c++ {
			if e := kerr.TypedErrorForCode(c); e != nil && e.Code == c && e.Message == name {
				code = c
			}
		}
		info := Explain(code)
		if strings.HasPrefix(info.Name, "ERROR_") {
			t.Errorf("code %d has no name", code)
		}
		if info.Cause == "" || info.Fix == "" {
			t.Errorf("%s (%d) is returned by the broker and has no row in the error table", name, code)
		}
	}
}

func TestErrorTableNamesAreTheWireNames(t *testing.T) {
	seen := map[int16]bool{}
	for _, code := range ErrorCodes() {
		if seen[code] {
			t.Errorf("code %d has two rows", code)
		}
		seen[code] = true
		if got, want := Explain(code).Name, kerr.TypedErrorForCode(code).Message; got != want {
			t.Errorf("code %d is %q, the wire's name is %q", code, got, want)
		}
		if Explain(code).Meaning == "" {
			t.Errorf("code %d has no meaning", code)
		}
	}
	if got := Explain(9999); got.Name != "ERROR_9999" || got.Cause != "" || got.Class != ClassRefused {
		t.Errorf("an unknown code is %+v", got)
	}
	if got := Explain(0); got.Class != ClassOK || got.Name != "" {
		t.Errorf("code 0 is %+v", got)
	}
}
