package meta

import (
	"strings"
	"testing"
)

// An entry is stored under a key built from all seven fields, so two distinct
// entries must have distinct keys. The broker refuses a NUL (the separator) in
// any text field at creation, so the property is for NUL-free entries.
func FuzzACLKey(f *testing.F) {
	f.Add(int8(2), "orders", int8(3), "User:alice", "*", int8(3), int8(3),
		int8(2), "orders", int8(3), "User:alice", "*", int8(3), int8(2))
	f.Add(int8(2), "a", int8(3), "b", "c", int8(1), int8(1), int8(2), "a", int8(3), "b", "c", int8(1), int8(1))
	f.Fuzz(func(t *testing.T,
		rt1 int8, n1 string, pt1 int8, p1, h1 string, op1, pm1 int8,
		rt2 int8, n2 string, pt2 int8, p2, h2 string, op2, pm2 int8) {
		a := ACL{rt1, n1, pt1, p1, h1, op1, pm1}
		b := ACL{rt2, n2, pt2, p2, h2, op2, pm2}
		if strings.ContainsRune(n1+p1+h1+n2+p2+h2, 0) {
			t.Skip("a NUL is refused at creation")
		}
		same := a == b
		if sameKey := string(a.key()) == string(b.key()); sameKey != same {
			t.Fatalf("entries %+v and %+v: equal=%v but keys equal=%v", a, b, same, sameKey)
		}
	})
}
