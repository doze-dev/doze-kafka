package conformance

import (
	"encoding/json"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// What Apache Kafka answers to ApiVersions: to the name and version a client gives
// for itself (from v3), and to a version of the request it does not serve. It is the
// first request of every client, and the one request a broker answers before it knows
// what the client speaks.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "apiversions-client-software", Run: apiVersionsClientSoftware},
		Scenario{Name: "apiversions-unsupported-version", Run: apiVersionsUnsupportedVersion},
	)
}

// apiVersionsAsk is an ApiVersions request whose answer is read as the broker frames
// it. A version the broker does not serve is answered in the v0 form, whatever was
// asked (the client could not read anything else), so the answer to one is decoded at
// v0; v0 says so.
type apiVersionsAsk struct {
	*kmsg.ApiVersionsRequest
	v0 bool
}

func (a apiVersionsAsk) ResponseKind() kmsg.Response {
	return &apiVersionsAnswer{ApiVersionsResponse: kmsg.NewPtrApiVersionsResponse(), v0: a.v0}
}

// apiVersionsAnswer is the answer, reduced to what these scenarios are about: the
// error, and the list of APIs when it is short enough to be a statement (none, or
// ApiVersions alone). A full list is the broker's whole API table, which is another
// document's business (docs/api-support.md); of it, only the row for ApiVersions
// itself is kept, and that there were others.
type apiVersionsAnswer struct {
	*kmsg.ApiVersionsResponse
	v0 bool
}

func (a *apiVersionsAnswer) SetVersion(v int16) {
	if a.v0 {
		v = 0
	}
	a.ApiVersionsResponse.SetVersion(v)
}

func (a *apiVersionsAnswer) MarshalJSON() ([]byte, error) {
	type row struct {
		ApiKey, MinVersion, MaxVersion int16
	}
	out := struct {
		ErrorCode    int16
		ApiKeys      []row
		OtherApiKeys bool
	}{ErrorCode: a.ErrorCode, ApiKeys: []row{}}
	for _, k := range a.ApiKeys {
		if len(a.ApiKeys) <= 1 || k.ApiKey == int16(kmsg.ApiVersions) {
			out.ApiKeys = append(out.ApiKeys, row{k.ApiKey, k.MinVersion, k.MaxVersion})
		} else {
			out.OtherApiKeys = true
		}
	}
	return json.Marshal(out)
}

func (e *Env) apiVersions(label string, version int16, name, softwareVersion string) int16 {
	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName, req.ClientSoftwareVersion = name, softwareVersion
	resp := e.Do(label, apiVersionsAsk{ApiVersionsRequest: req}, version)
	if a, ok := resp.(*apiVersionsAnswer); ok {
		return a.ErrorCode
	}
	return -1
}

// apiVersionsClientSoftware asks what a broker makes of the client software name and
// version of ApiVersions v3 and later: which it takes, which it refuses and with what,
// and whether the connection lives on.
func apiVersionsClientSoftware(e *Env) {
	e.apiVersions("v3-java-client", 3, "apache-kafka-java", "3.7.0")
	e.apiVersions("v4-java-client", 4, "apache-kafka-java", "4.3.1")
	e.apiVersions("v3-librdkafka", 3, "librdkafka", "2.15.1")
	e.apiVersions("v3-one-character-each", 3, "a", "1")
	e.apiVersions("v3-dots-and-dashes-inside", 3, "my.app-name", "1.0-rc.1")

	// Before v3 the request has no such fields.
	for _, v := range []int16{0, 1, 2} {
		e.apiVersions("v"+string(rune('0'+v))+"-no-software-fields", v, "", "")
	}

	e.apiVersions("v3-empty-name-and-version", 3, "", "")
	e.apiVersions("v3-empty-name", 3, "", "1.0")
	e.apiVersions("v3-empty-version", 3, "my-app", "")
	e.apiVersions("v3-name-with-a-space", 3, "my app", "1.0")
	e.apiVersions("v3-name-with-an-underscore", 3, "my_app", "1.0")
	e.apiVersions("v3-name-begins-with-a-dash", 3, "-app", "1.0")
	e.apiVersions("v3-name-ends-with-a-dot", 3, "app.", "1.0")
	e.apiVersions("v3-name-not-ascii", 3, "café", "1.0")
	e.apiVersions("v3-name-with-a-slash", 3, "my/app", "1.0")
	e.apiVersions("v3-version-with-a-plus", 3, "my-app", "1.0+build5")
	e.apiVersions("v3-version-with-a-space", 3, "my-app", "1.0 beta")
	e.apiVersions("v3-version-ends-with-a-dash", 3, "my-app", "1.0-")
	e.apiVersions("v4-name-with-a-space", 4, "my app", "1.0")

	// The connection that was refused is the one these went over: it is still served.
	e.apiVersions("v3-valid-after-the-refusals", 3, "my-app", "1.0")
	md := kmsg.NewPtrMetadataRequest()
	md.Topics = []kmsg.MetadataRequestTopic{}
	e.Do("metadata-after-the-refusals", md, 9)

	// Long, but of the characters allowed: is there a length it refuses?
	e.apiVersions("v3-name-of-200-characters", 3, strings.Repeat("a", 200), "1.0")
	e.apiVersions("v3-name-of-20000-characters", 3, strings.Repeat("a", 20000), "1.0")
	e.apiVersions("v3-version-of-20000-characters", 3, "my-app", strings.Repeat("1", 20000))
}

// apiVersionsUnsupportedVersion asks for ApiVersions at versions past the last one a
// broker serves. The answer is how a client newer than the broker learns what to
// speak, so its shape is part of the protocol: which error, in which framing, and
// which APIs it lists.
func apiVersionsUnsupportedVersion(e *Env) {
	ask := func(label string, version int16, name, softwareVersion string, v0 bool) {
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName, req.ClientSoftwareVersion = name, softwareVersion
		e.Do(label, apiVersionsAsk{ApiVersionsRequest: req, v0: v0}, version)
	}
	ask("highest-served", 4, "my-app", "1.0", false)
	ask("one-past-the-highest", 5, "my-app", "1.0", true)
	ask("far-past-the-highest", 99, "my-app", "1.0", true)
	ask("highest-a-version-can-be", 32767, "my-app", "1.0", true)
	// The body of a version the broker does not know is not read: a name it would
	// refuse at v3 changes nothing.
	ask("past-the-highest-with-a-name-it-would-refuse", 5, "my app", "1.0", true)
	// And the connection is still served afterwards.
	ask("served-again-after-them", 3, "my-app", "1.0", false)
}
