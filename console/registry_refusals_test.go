package console

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// refusingRegistryConsole is a console whose registry wants alice's password, signing
// in as user (none when empty) with a password that is wrong.
func refusingRegistryConsole(t *testing.T, user string, facts BrokerFacts) (*Console, *httptest.Server) {
	t.Helper()
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(schemaregistry.NewAPI(reg, schemaregistry.Options{Auth: func(u, p string) bool { return u == "alice" && p == "right" }}))
	t.Cleanup(ts.Close)
	c, _ := newTestConsoleAddr(t, func(o *Options) {
		o.RegistryURL, o.RegistryUser, o.RegistryPass = ts.URL, user, "wrong"
		o.Facts = facts
	})
	return c, ts
}

func refusedPage(t *testing.T, c *Console) string {
	t.Helper()
	rec := req(t, c, "GET", "/_console/schemas", nil)
	if rec.Code != 401 {
		t.Fatalf("a refused console: %d %s", rec.Code, rec.Body)
	}
	return readable(rec.Body.String())
}

const (
	registryRuleSASL = "SASL is on, so the registry requires HTTP basic authentication with the same user names and passwords as the broker."
	registryRuleOpen = "Rule: the registry requires HTTP basic authentication. A Confluent registry"
)

// The refusal page says the rule in the broker's terms when the broker has SASL, whether
// it is the console's user or the mechanisms that tell, and in the registry's own when not.
func TestTheRefusedRegistryPageSaysTheRuleInTheBrokersTermsOnlyWithSASL(t *testing.T) {
	c, _ := refusingRegistryConsole(t, "checkout", BrokerFacts{})
	if page := refusedPage(t, c); !strings.Contains(page, registryRuleSASL) || strings.Contains(page, registryRuleOpen) {
		t.Errorf("a console signed in as a user:\n%s", between(page, "Rule", "Nothing was changed"))
	}
	c, _ = refusingRegistryConsole(t, "", BrokerFacts{SASLMechanisms: []string{"PLAIN"}})
	if page := refusedPage(t, c); !strings.Contains(page, registryRuleSASL) || strings.Contains(page, registryRuleOpen) {
		t.Errorf("a broker that serves SASL mechanisms:\n%s", between(page, "Rule", "Nothing was changed"))
	}
	c, _ = refusingRegistryConsole(t, "", BrokerFacts{})
	page := refusedPage(t, c)
	if !strings.Contains(page, registryRuleOpen) || strings.Contains(page, registryRuleSASL) {
		t.Errorf("a console with no user on an open broker:\n%s", between(page, "Rule", "Nothing was changed"))
	}
	if !strings.Contains(page, "The console sent no user: the broker has SASL off, and this registry wants one.") || !strings.Contains(page, "basic.auth.user.info=<user>:<password>") {
		t.Errorf("a console with no user:\n%s", between(page, "The console's own sign-in", "Check the same"))
	}
}

// The refusal page gives the address clients reach the registry at, when the broker knows
// one that is not the console's, and otherwise the console's own.
func TestTheRefusedRegistryPageGivesTheAddressClientsUse(t *testing.T) {
	c, _ := refusingRegistryConsole(t, "checkout", BrokerFacts{RegistryURL: "http://registry.zone.test:8081"})
	page := refusedPage(t, c)
	for _, want := range []string{"The registry at registry.zone.test:8081 is up", "schema.registry.url=http://registry.zone.test:8081", "http://registry.zone.test:8081/subjects"} {
		if !strings.Contains(page, want) {
			t.Errorf("the refusal lacks %q:\n%s", want, page)
		}
	}
	c, ts := refusingRegistryConsole(t, "checkout", BrokerFacts{})
	page = refusedPage(t, c)
	for _, want := range []string{"The registry at " + strings.TrimPrefix(ts.URL, "http://") + " is up", "schema.registry.url=" + ts.URL, ts.URL + "/subjects"} {
		if !strings.Contains(page, want) {
			t.Errorf("the refusal lacks %q:\n%s", want, page)
		}
	}
}

// A soft-deleted subject listed on the Schemas page carries the confirm for removing it
// for good, which says how many versions go with it.
func TestTheSoftDeletedListSaysHowManyVersionsGoWithEachSubject(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "kept-value", userV1)
	register(t, c, "gone-value", userV1)
	register(t, c, "gone-value", userV2)
	if err := c.reg.SoftDeleteSubject(t.Context(), "gone-value"); err != nil {
		t.Fatal(err)
	}
	section := between(req(t, c, "GET", "/_console/schemas", nil).Body.String(), `id="list-deleted"`, "</aside>")
	if !strings.Contains(section, "All 2 versions") {
		t.Errorf("the soft-deleted subject's confirm:\n%s", section)
	}
}

// An empty registry's page shows how a serializer registers, with the first topic of the
// broker as its example, and a placeholder when the broker has none.
func TestTheEmptyRegistryPageUsesATopicOfTheBrokerAsItsExample(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	if page := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(page, "POST /subjects/<topic>-value/versions") {
		t.Errorf("no topics:\n%s", between(page, "How a stock", "Or register"))
	}
	for _, name := range []string{"orders", "payments"} {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {name}, "partitions": {"1"}})
	}
	if page := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(page, "POST /subjects/orders-value/versions") {
		t.Errorf("topics orders and payments:\n%s", between(page, "How a stock", "Or register"))
	}
}
