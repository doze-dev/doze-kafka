package schemaregistry

import "sync"

// MemoryStorage is a Storage that keeps its records in memory: for tests, and for a
// registry that need not outlive its process. Opening a second registry over the same
// MemoryStorage shows what a restart would find.
type MemoryStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{m: map[string][]byte{}} }

func (s *MemoryStorage) LoadAll() (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.m))
	for k, v := range s.m {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}

func (s *MemoryStorage) Apply(puts map[string][]byte, deletes []string) error {
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
