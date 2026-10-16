package broker

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// An entry's identity in the store is its fields joined with NUL, so a NUL inside a
// name, principal or host would make two distinct entries one (ACC-31): creating
// the second would silently replace the first, and deleting one would delete both.
// A NUL is refused at creation instead.
func TestCreateACLRefusesANulInAnyText(t *testing.T) {
	good := acl(resTopic, "orders", aclLiteral, "User:alice", "*", 3, 3)
	if code, why := validateCreation(good); code != 0 {
		t.Fatalf("the control entry is refused: %s", why)
	}
	for field, mutate := range map[string]func(a meta.ACL) meta.ACL{
		"name":      func(a meta.ACL) meta.ACL { a.Name = "ord\x00ers"; return a },
		"principal": func(a meta.ACL) meta.ACL { a.Principal = "User:al\x00ice"; return a },
		"host":      func(a meta.ACL) meta.ACL { a.Host = "10.0.0.1\x00*"; return a },
	} {
		if code, _ := validateCreation(mutate(good)); code == 0 {
			t.Errorf("an entry with a NUL in its %s was accepted", field)
		}
	}
}

// Foundation B12: ACL filters come from DescribeACLs and DeleteACLs as they are.
// Two properties hold for every filter and entry, whatever the bytes: a filter
// that asks for everything matches everything, and a filter written from an entry
// (every field exact) matches that entry.
func FuzzACLFilterMatches(f *testing.F) {
	f.Add(int8(2), "orders", int8(3), "User:alice", "*", int8(3), int8(3))
	f.Add(int8(2), "ord", int8(4), "User:*", "10.0.0.1", int8(2), int8(2))
	f.Add(int8(-1), "", int8(0), "", "", int8(127), int8(-128))
	f.Fuzz(func(t *testing.T, rt int8, name string, pt int8, principal, host string, op, perm int8) {
		entry := meta.ACL{ResourceType: rt, Name: name, PatternType: pt, Principal: principal, Host: host, Operation: op, Permission: perm}

		everything := aclFilter{resourceType: aclAny, patternType: aclAny, operation: aclAny, permission: aclAny}
		if !everything.matches(entry) {
			t.Fatalf("the filter for everything does not match %+v", entry)
		}
		exact := aclFilter{resourceType: rt, name: &name, patternType: pt, principal: &principal, host: &host, operation: op, permission: perm}
		if !exact.matches(entry) && pt != aclMatch {
			t.Fatalf("a filter written from %+v does not match it", entry)
		}
		// Relaxing one field of a matching filter never makes it stop matching.
		if exact.matches(entry) {
			relaxed := exact
			relaxed.name = nil
			if !relaxed.matches(entry) {
				t.Fatalf("dropping the name from a matching filter stopped it matching %+v", entry)
			}
			relaxed = exact
			relaxed.principal, relaxed.host = nil, nil
			if !relaxed.matches(entry) {
				t.Fatalf("dropping the principal and host from a matching filter stopped it matching %+v", entry)
			}
		}
		_ = exact.validate()
		_, _ = validateCreation(entry)
		if strings.Contains(name+principal+host, "\x00") && first(validateCreation(entry)) == 0 {
			t.Fatalf("an entry with a NUL passed validateCreation: %+v", entry)
		}
	})
}

func first(code int16, _ string) int16 { return code }
