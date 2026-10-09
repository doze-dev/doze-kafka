package meta

import "go.etcd.io/bbolt"

func (s *boltStore) RegistryAll() (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketRegistry).ForEach(func(k, v []byte) error {
			out[string(k)] = append([]byte(nil), v...)
			return nil
		})
	})
	return out, err
}

func (s *boltStore) RegistryApply(puts map[string][]byte, deletes []string) error {
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRegistry)
		for _, k := range deletes {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		for k, v := range puts {
			if err := b.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}
