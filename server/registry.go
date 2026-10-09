package server

import (
	"crypto/subtle"
	"net/http"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// metaStorage keeps the registry's records in the broker's metadata database, so a
// restart finds them and no second store is needed.
type metaStorage struct {
	s interface {
		RegistryAll() (map[string][]byte, error)
		RegistryApply(puts map[string][]byte, deletes []string) error
	}
}

func (m metaStorage) LoadAll() (map[string][]byte, error) { return m.s.RegistryAll() }
func (m metaStorage) Apply(puts map[string][]byte, deletes []string) error {
	return m.s.RegistryApply(puts, deletes)
}

// SchemaRegistry is the built-in Confluent-compatible schema registry as an HTTP
// handler. Its schemas are stored in the broker's metadata database. When the broker
// requires SASL, the registry requires the same user names and passwords as HTTP
// basic authentication. The handler is valid until the server is closed.
func (s *Server) SchemaRegistry() (http.Handler, error) {
	reg, err := schemaregistry.Open(metaStorage{s.meta})
	if err != nil {
		return nil, err
	}
	opts := schemaregistry.Options{Logger: s.log}
	if s.cfg.AuthEnabled() {
		creds := s.cfg.Credentials
		opts.Auth = func(user, pass string) bool {
			want, ok := creds[user]
			return ok && subtle.ConstantTimeCompare([]byte(want), []byte(pass)) == 1
		}
	}
	return schemaregistry.NewAPI(reg, opts), nil
}
