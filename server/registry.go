package server

import (
	"crypto/subtle"
	"net"
	"net/http"

	"github.com/doze-dev/doze-kafka/internal/config"
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

// registryHosts is every host name, beyond the ones the registry always answers to
// (IP literals, localhost, *.doze, which covers its own schema.<instance>.doze), that
// a request's Host header may name:
//
//   - the host clients are told to dial (AdvertisedHost): a client that reaches the
//     broker under that name reaches the registry beside it under the same one
//     (a container reaching the host as host.docker.internal, a compose service
//     name), and it is a name the operator chose, not one a web page can choose;
//   - whatever the operator listed for the console (--console-allow-host): one list
//     of the names this instance is reached by, "*" included;
//   - the name in the registry's own address when it is a name and not an address
//     (binding to "devbox.lan:8081" means expecting to be reached as it), as the
//     console does with its own.
//
// Binding to every interface adds nothing: an address is always allowed and a name
// has to be one of the above, as for the console.
func registryHosts(cfg config.Config) []string {
	hosts := append([]string(nil), cfg.ConsoleAllowHosts...)
	if cfg.AdvertisedHost != "" {
		hosts = append(hosts, cfg.AdvertisedHost)
	}
	if h, _, err := net.SplitHostPort(cfg.SchemaRegistryAddr); err == nil && h != "" && net.ParseIP(h) == nil {
		hosts = append(hosts, h)
	}
	return hosts
}

// SchemaRegistry is the built-in Confluent-compatible schema registry as an HTTP
// handler. Its schemas are stored in the broker's metadata database. When the broker
// requires SASL, the registry requires the same user names and passwords as HTTP
// basic authentication. The handler is valid until the server is closed.
//
// The handler answers only requests whose Host header is an IP address, localhost,
// a name under .doze, or Options.AdvertisedHost; any other gets 403 (the defence
// against DNS rebinding from a web page). Serve it under another name by setting
// AdvertisedHost to that name.
func (s *Server) SchemaRegistry() (http.Handler, error) {
	reg, err := schemaregistry.Open(metaStorage{s.meta})
	if err != nil {
		return nil, err
	}
	opts := schemaregistry.Options{Logger: s.log, AllowedHosts: registryHosts(s.cfg)}
	if s.cfg.AuthEnabled() {
		creds := s.cfg.Credentials
		opts.Auth = func(user, pass string) bool {
			want, ok := creds[user]
			return ok && subtle.ConstantTimeCompare([]byte(want), []byte(pass)) == 1
		}
	}
	return schemaregistry.NewAPI(reg, opts), nil
}
