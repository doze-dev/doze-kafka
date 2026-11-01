package protocol

import (
	"context"
	"encoding/binary"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// canaried fills every field of a request, however deep, with something that can be
// looked for: a string is "CANARY-" and its field's name, a run of bytes is 37 bytes
// (a length nothing else in the output has), a list has one item.
func canaried(v reflect.Value, field string) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		canaried(v.Elem(), field)
	case reflect.String:
		v.SetString("CANARY-" + field)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			v.SetBytes([]byte(strings.Repeat("s", 37-len(field)) + field)[:37])
			return
		}
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		canaried(v.Index(0), field)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if !f.IsExported() || f.Name == "Version" || f.Name == "UnknownTags" {
				continue
			}
			canaried(v.Field(i), f.Name)
		}
	}
}

// authRequests is a canaried request of every API that can carry a credential.
func authRequests(t *testing.T) map[string]kmsg.Request {
	t.Helper()
	out := map[string]kmsg.Request{}
	for key := int16(0); key < 200; key++ {
		q := kmsg.RequestForKey(key)
		if q == nil || !IsAuthAPI(APIKey(key)) {
			continue
		}
		q.SetVersion(q.MaxVersion())
		canaried(reflect.ValueOf(q).Elem(), "")
		out[reflect.TypeOf(q).Elem().Name()] = q
	}
	return out
}

// For a request that carries a credential the inspector shows the fields on a list and
// hides every other, whatever it holds: the name is there, the value is "(hidden)". A
// run of bytes is never shown, and neither is its length: the length of a SASL/PLAIN
// token is the length of the password, give or take the user's name beside it.
func TestTheInspectorShowsOnlyListedFieldsOfARequestThatCarriesACredential(t *testing.T) {
	want := map[string]string{
		"SASLHandshakeRequest":    `{"Mechanism": "CANARY-Mechanism"}`,
		"SASLAuthenticateRequest": `{"AuthBytes": "(hidden)"}`,
		"CreateDelegationTokenRequest": `{"OwnerPrincipalType": "CANARY-OwnerPrincipalType", "OwnerPrincipalName": "CANARY-OwnerPrincipalName", ` +
			`"Renewers": [{"PrincipalType": "CANARY-PrincipalType", "PrincipalName": "CANARY-PrincipalName"}], "MaxLifetimeMillis": 7}`,
		"RenewDelegationTokenRequest":         `{"HMAC": "(hidden)", "RenewTimeMillis": 7}`,
		"ExpireDelegationTokenRequest":        `{"HMAC": "(hidden)", "ExpiryPeriodMillis": 7}`,
		"DescribeDelegationTokenRequest":      `{"Owners": [{"PrincipalType": "CANARY-PrincipalType", "PrincipalName": "CANARY-PrincipalName"}]}`,
		"DescribeUserSCRAMCredentialsRequest": `{"Users": [{"Name": "CANARY-Name"}]}`,
		"AlterUserSCRAMCredentialsRequest": `{"Deletions": [{"Name": "CANARY-Name", "Mechanism": 7}], ` +
			`"Upsertions": [{"Name": "CANARY-Name", "Mechanism": 7, "Iterations": 7, "Salt": "(hidden)", "SaltedPassword": "(hidden)"}]}`,
	}
	reqs := authRequests(t)
	if len(reqs) != len(want) {
		t.Errorf("%d APIs carry a credential and %d are expected here", len(reqs), len(want))
	}
	length := regexp.MustCompile(`\d+ (bytes|B|items)\b`)
	for name, q := range reqs {
		got := DescribeFacts(q, nil).Decoded
		if got != want[name] {
			t.Errorf("%s:\n got %s\nwant %s", name, got, want[name])
		}
		if m := length.FindString(got); m != "" {
			t.Errorf("%s: the inspector shows a length (%q): %s", name, m, got)
		}
	}
}

// The list is of what may be shown: a field nobody listed is hidden, and so is the
// whole of a type nobody listed. A string field added to one of these requests by a
// later protocol version is hidden until someone decides it is not a secret.
func TestAFieldThatIsNotListedIsHidden(t *testing.T) {
	for name, q := range authRequests(t) {
		got := DescribeFacts(q, nil).Decoded
		for _, m := range regexp.MustCompile(`CANARY-(\w+)`).FindAllStringSubmatch(got, -1) {
			listed := false
			for typ, fields := range authShown {
				if strings.HasPrefix(typ, strings.TrimSuffix(name, "Request")) {
					for _, f := range fields {
						listed = listed || f == m[1]
					}
				}
			}
			if !listed {
				t.Errorf("%s shows the field %s, which authShown does not list: %s", name, m[1], got)
			}
		}
	}

	// Taken off the list, a field is hidden; with its type off the list, all of it is.
	q := kmsg.NewPtrSASLHandshakeRequest()
	q.Mechanism = "CANARY-Mechanism"
	old := authShown["SASLHandshakeRequest"]
	defer func() { authShown["SASLHandshakeRequest"] = old }()
	authShown["SASLHandshakeRequest"] = nil
	if got := DescribeFacts(q, nil).Decoded; got != `{"Mechanism": "(hidden)"}` {
		t.Errorf("a field off the list: %s", got)
	}
	delete(authShown, "SASLHandshakeRequest")
	if got := DescribeFacts(q, nil).Decoded; got != `{"Mechanism": "(hidden)"}` {
		t.Errorf("a type off the list: %s", got)
	}

	// A run of bytes is hidden even if someone lists it, and its length with it.
	a := kmsg.NewPtrSASLAuthenticateRequest()
	a.SASLAuthBytes = []byte("\x00alice\x00her-password")
	oldAuth := authShown["SASLAuthenticateRequest"]
	defer func() { authShown["SASLAuthenticateRequest"] = oldAuth }()
	authShown["SASLAuthenticateRequest"] = []string{"SASLAuthBytes"}
	if got := DescribeFacts(a, nil).Decoded; got != `{"AuthBytes": "(hidden)"}` {
		t.Errorf("bytes that were listed: %s", got)
	}
}

// A hidden field is hidden the same way whether it is set or not: an absent line would
// say the token was empty.
func TestAHiddenFieldIsShownHiddenWhenItIsEmpty(t *testing.T) {
	if got := DescribeFacts(kmsg.NewPtrSASLAuthenticateRequest(), nil).Decoded; got != `{"AuthBytes": "(hidden)"}` {
		t.Errorf("an empty token: %s", got)
	}
}

// Every request whose name or whose fields say it holds a credential is one of the
// APIs held to the list. A new API of that kind in the message definitions fails here
// until it is added to IsAuthAPI.
func TestEveryRequestThatHoldsACredentialIsHeldToTheList(t *testing.T) {
	credential := regexp.MustCompile(`(?i)sasl|scram|delegationtoken|password|secret|hmac|salt|credential|authbytes`)
	var holds func(t reflect.Type, seen map[reflect.Type]bool) string
	holds = func(typ reflect.Type, seen map[reflect.Type]bool) string {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return ""
		}
		seen[typ] = true
		if credential.MatchString(typ.Name()) {
			return "the type " + typ.Name()
		}
		for i := 0; i < typ.NumField(); i++ {
			if f := typ.Field(i); f.IsExported() {
				if credential.MatchString(f.Name) {
					return "the field " + typ.Name() + "." + f.Name
				}
				if why := holds(f.Type, seen); why != "" {
					return why
				}
			}
		}
		return ""
	}
	found := 0
	for key := int16(0); key < 200; key++ {
		q := kmsg.RequestForKey(key)
		if q == nil {
			continue
		}
		why := holds(reflect.TypeOf(q), map[reflect.Type]bool{})
		if why != "" {
			found++
		}
		if (why != "") != IsAuthAPI(APIKey(key)) {
			t.Errorf("%s: holds a credential = %q, IsAuthAPI = %v", APIName(APIKey(key)), why, IsAuthAPI(APIKey(key)))
		}
	}
	if found < 8 {
		t.Errorf("only %d requests were found to hold a credential: the search is not looking", found)
	}
	// And each of their types is on the list, so that what it shows was decided.
	for name, q := range authRequests(t) {
		var walk func(reflect.Type)
		walk = func(typ reflect.Type) {
			for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
				typ = typ.Elem()
			}
			if typ.Kind() != reflect.Struct {
				return
			}
			listed, ok := authShown[typ.Name()]
			if !ok {
				t.Errorf("%s: the type %s is not in authShown", name, typ.Name())
			}
			// What is listed is a field the type has, and never a run of bytes.
			for _, l := range listed {
				f, has := typ.FieldByName(l)
				if !has {
					t.Errorf("authShown lists %s.%s, which is not a field", typ.Name(), l)
				} else if (f.Type.Kind() == reflect.Slice || f.Type.Kind() == reflect.Array) && f.Type.Elem().Kind() == reflect.Uint8 {
					t.Errorf("authShown lists %s.%s, which is bytes", typ.Name(), l)
				}
			}
			for i := 0; i < typ.NumField(); i++ {
				if f := typ.Field(i); f.IsExported() && f.Name != "UnknownTags" {
					walk(f.Type)
				}
			}
		}
		walk(reflect.TypeOf(q))
	}
}

// The size of a SASLAuthenticate request is the size of its token plus what the row
// already shows (the client id), so the row does not have it; the size of the answer,
// which no secret decides, it does have.
func TestTheSizeOfASASLTokenIsNotObserved(t *testing.T) {
	router := NewRouter()
	answer := func(_ context.Context, req *Request) ([]byte, error) {
		return binary.BigEndian.AppendUint32(nil, uint32(req.Header.CorrelationID)), nil
	}
	router.Register(kmsg.SASLAuthenticate, answer)
	router.Register(kmsg.SASLHandshake, answer)
	conn, obs := serveObserved(t, router)
	token := []byte("\x00alice\x00a-password-of-some-length")
	sendRaw(t, conn, kmsg.SASLHandshake, 1, 1, "app", []byte{0, 5, 'P', 'L', 'A', 'I', 'N'})
	sendRaw(t, conn, kmsg.SASLAuthenticate, 1, 2, "app", append(binary.BigEndian.AppendUint32(nil, uint32(len(token))), token...))
	got := waitObservations(t, obs, 2)
	if h := got[0]; h.API != kmsg.SASLHandshake || h.ReqBytes == 0 || h.RespBytes == 0 {
		t.Errorf("the handshake: %+v", h)
	}
	if a := got[1]; a.API != kmsg.SASLAuthenticate || a.ReqBytes != 0 || a.RespBytes == 0 {
		t.Errorf("the SASLAuthenticate request is observed with %d bytes in and %d out; want 0 in (not recorded) and its answer's size out", a.ReqBytes, a.RespBytes)
	}

	// One that ends the connection (no handler here) is observed the same way.
	conn, obs = serveObserved(t, NewRouter())
	sendRaw(t, conn, kmsg.SASLAuthenticate, 1, 2, "app", append(binary.BigEndian.AppendUint32(nil, uint32(len(token))), token...))
	if a := waitObservations(t, obs, 1)[0]; a.Closed == "" || a.ReqBytes != 0 {
		t.Errorf("a SASLAuthenticate request that ended its connection is observed with %d bytes in (closed: %q)", a.ReqBytes, a.Closed)
	}
}
