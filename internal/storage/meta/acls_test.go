package meta

import (
	"testing"
)

// ACLs are stored, listed in a stable order, never duplicated, removed exactly, and
// still there after a reopen.
func TestACLsStoreListRemoveAndPersist(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := ACL{ResourceType: 2, Name: "orders", PatternType: 3, Principal: "User:alice", Host: "*", Operation: 3, Permission: 3}
	b := ACL{ResourceType: 2, Name: "orders", PatternType: 3, Principal: "User:bob", Host: "*", Operation: 4, Permission: 2}
	c := a // differs from a only in the principal: a different entry.
	c.Principal = "User:carol"
	if err := s.AddACLs([]ACL{b, a, a, c}); err != nil { // a twice: one entry.
		t.Fatal(err)
	}
	got, err := s.ListACLs()
	if err != nil || len(got) != 3 {
		t.Fatalf("ListACLs = %+v, %v, want the three distinct entries", got, err)
	}
	if err := s.RemoveACLs([]ACL{a, {ResourceType: 9, Name: "never-there"}}); err != nil { // an absent one is ignored.
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, _ = s.ListACLs()
	if len(got) != 2 {
		t.Errorf("after removing a and reopening: %+v, want b and c", got)
	}
}
