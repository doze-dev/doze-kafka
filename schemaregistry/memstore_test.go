package schemaregistry

import "sync"

// memStore is a Storage that keeps its records in memory, so tests can reopen a
// registry over the same records to look at what survives a restart.
type memStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemStore() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) LoadAll() (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.m))
	for k, v := range s.m {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}

func (s *memStore) Apply(puts map[string][]byte, deletes []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range deletes {
		delete(s.m, k)
	}
	for k, v := range puts {
		s.m[k] = append([]byte(nil), v...)
	}
	return nil
}
