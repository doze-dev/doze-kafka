package protocol

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// What the Traffic inspector shows of a request that carries a credential (IsAuthAPI).
//
// Everywhere else the inspector shows a field unless its name says it is a secret. For
// these requests it is the other way round: a field is shown only if it is on the list
// below, and every other field is written with its name and "(hidden)" whether it is
// set or not. A run of bytes is never shown and neither is its length: a SASL/PLAIN
// token is the user's name and the password, so its length is the password's.

// authShown lists, for each type these requests are made of, the fields that may be
// shown. A type that is not here shows none of its fields.
var authShown = map[string][]string{
	"SASLHandshakeRequest":    {"Mechanism"},
	"SASLAuthenticateRequest": {},

	"CreateDelegationTokenRequest":        {"OwnerPrincipalType", "OwnerPrincipalName", "Renewers", "MaxLifetimeMillis"},
	"CreateDelegationTokenRequestRenewer": {"PrincipalType", "PrincipalName"},
	"RenewDelegationTokenRequest":         {"RenewTimeMillis"},
	"ExpireDelegationTokenRequest":        {"ExpiryPeriodMillis"},
	"DescribeDelegationTokenRequest":      {"Owners"},
	"DescribeDelegationTokenRequestOwner": {"PrincipalType", "PrincipalName"},

	"DescribeUserSCRAMCredentialsRequest":       {"Users"},
	"DescribeUserSCRAMCredentialsRequestUser":   {"Name"},
	"AlterUserSCRAMCredentialsRequest":          {"Deletions", "Upsertions"},
	"AlterUserSCRAMCredentialsRequestDeletion":  {"Name", "Mechanism"},
	"AlterUserSCRAMCredentialsRequestUpsertion": {"Name", "Mechanism", "Iterations"},
}

const hiddenValue = `"(hidden)"`

// authDecodedFields is decodedFields for a request that carries a credential.
func authDecodedFields(q kmsg.Request) string {
	v := reflect.ValueOf(q)
	for _, lim := range []struct{ items, depth int }{{8, 8}, {4, 6}, {2, 5}, {1, 4}, {1, 3}} {
		var b strings.Builder
		writeAuthValue(&b, v, lim.items, lim.depth)
		if b.Len() <= maxDecodedBytes {
			return b.String()
		}
	}
	return `{"…": "the request is too large to show here"}`
}

// writeAuthValue writes a value of a listed field: a struct by its own list, a list of
// structs item by item, a number or a string as it is, and anything made of bytes not
// at all.
func writeAuthValue(b *strings.Builder, v reflect.Value, items, depth int) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			b.WriteString("null")
			return
		}
		writeAuthValue(b, v.Elem(), items, depth)
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String:
		writeValue(b, v, items, depth)
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 || depth <= 0 {
			b.WriteString(hiddenValue)
			return
		}
		b.WriteByte('[')
		n := min(v.Len(), items)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			writeAuthValue(b, v.Index(i), items, depth-1)
		}
		if v.Len() > n {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote("… " + strconv.Itoa(v.Len()-n) + " more"))
		}
		b.WriteByte(']')
	case reflect.Struct:
		if depth <= 0 {
			b.WriteString(hiddenValue)
			return
		}
		t := v.Type()
		shown := authShown[t.Name()]
		b.WriteByte('{')
		first := true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() || f.Name == "Version" || f.Name == "UnknownTags" {
				continue
			}
			name := specFieldName(t, f.Name)
			if name == "" {
				continue
			}
			listed := false
			for _, s := range shown {
				listed = listed || s == f.Name
			}
			fv := v.Field(i)
			// A listed field that is not set is left out, as everywhere else. A hidden
			// one is written either way: leaving it out would say it was empty.
			if listed && ((fv.Kind() == reflect.Pointer && fv.IsNil()) || (fv.Kind() == reflect.Slice && fv.Len() == 0)) {
				continue
			}
			if !first {
				b.WriteString(", ")
			}
			first = false
			b.WriteString(strconv.Quote(name))
			b.WriteString(": ")
			if listed {
				writeAuthValue(b, fv, items, depth-1)
			} else {
				b.WriteString(hiddenValue)
			}
		}
		b.WriteByte('}')
	default:
		b.WriteString(hiddenValue)
	}
}
