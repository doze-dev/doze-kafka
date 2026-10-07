package meta

import (
	"encoding/json"
	"strconv"
	"strings"

	"go.etcd.io/bbolt"
)

// ACL is one stored access-control entry, in Kafka's terms: an operation on a
// resource (a topic, a group, the cluster...) allowed or denied to a principal from a
// host. The integer fields are Kafka's wire values (kmsg.ACLResourceType and so on),
// kept as they are so a client gets back exactly what it sent.
//
// Entries are stored so that bootstrap scripts, Terraform and admin tools that
// provision ACLs complete and can read them back. They are NOT enforced: the broker
// never consults them, and every authenticated client can do everything.
type ACL struct {
	ResourceType int8   `json:"resource_type"`
	Name         string `json:"name"`
	PatternType  int8   `json:"pattern_type"`
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	Operation    int8   `json:"operation"`
	Permission   int8   `json:"permission"`
}

// key is what makes two entries the same entry. Every field takes part.
func (a ACL) key() []byte {
	return []byte(strings.Join([]string{
		strconv.Itoa(int(a.ResourceType)), a.Name, strconv.Itoa(int(a.PatternType)),
		a.Principal, a.Host, strconv.Itoa(int(a.Operation)), strconv.Itoa(int(a.Permission)),
	}, "\x00"))
}

func (s *boltStore) AddACLs(acls []ACL) error {
	if len(acls) == 0 {
		return nil
	}
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketACLs)
		for _, a := range acls {
			val, err := json.Marshal(a)
			if err != nil {
				return err
			}
			if err := b.Put(a.key(), val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *boltStore) ListACLs() ([]ACL, error) {
	var out []ACL
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketACLs).ForEach(func(_, v []byte) error {
			var a ACL
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			out = append(out, a)
			return nil
		})
	})
	return out, err // bbolt iterates in key order, which is stable.
}

func (s *boltStore) RemoveACLs(acls []ACL) error {
	if len(acls) == 0 {
		return nil
	}
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketACLs)
		for _, a := range acls {
			if err := b.Delete(a.key()); err != nil {
				return err
			}
		}
		return nil
	})
}
