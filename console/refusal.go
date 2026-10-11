package console

// One vocabulary for "no". Every error the console shows is a Refusal, rendered
// by one partial ("refusal" in templates/errors.html), in one of four words and
// one colour each:
//
//	Refused          real Kafka would say no too (a topic that exists, a bad name)
//	Denied           an ACL, or a login, decided
//	Not implemented  this broker does not do it
//	Failed           the broker is unhappy (storage, a timeout, no answer)
//
// A Refusal names what said no. For the broker that is the Kafka error name, taken
// from protocol.ErrorName, the table the wire protocol answers from, so the
// console cannot show a name the wire does not send. For the registry it is the numeric error_code.
// For a rule of the console's own (a field the form would not accept) there is no
// name, and the refusal says "Refused by the console" with the rule.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/twmb/franz-go/pkg/kerr"
)

// RefusalKind is one of the four words.
type RefusalKind string

const (
	Refused        RefusalKind = "refused"
	Denied         RefusalKind = "denied"
	NotImplemented RefusalKind = "not-implemented"
	Failed         RefusalKind = "failed"
)

// Word is the kind as the person reads it.
func (k RefusalKind) Word() string {
	switch k {
	case Refused:
		return "Refused"
	case Denied:
		return "Denied"
	case NotImplemented:
		return "Not implemented"
	}
	return "Failed"
}

// Refusal is an error as the person looking at the screen needs it.
type Refusal struct {
	Kind RefusalKind
	// Name is what said no: a Kafka error name ("TOPIC_ALREADY_EXISTS"), a registry
	// error_code ("42201"), or empty for a rule of the console's own.
	Name string
	// Code is the number behind Name (a Kafka error code, a registry error_code).
	Code int
	// Source is "kafka", "registry" or "console".
	Source string
	// Message says what happened, in the person's terms.
	Message string
	// Rule is the rule that decided: Kafka's own description of the error, or the
	// console's rule for what it would not send.
	Rule string

	// Status is the HTTP status the answer carries; Heading titles a full page.
	Status  int
	Heading string
	// NotFound marks "there is no such thing", which gets a page that helps (did you
	// mean) rather than a bare refusal.
	NotFound bool
	// Retry marks a failure worth trying again.
	Retry bool
}

// Source names.
const (
	SourceKafka    = "kafka"
	SourceRegistry = "registry"
	SourceConsole  = "console"
)

// kafkaRefusal is an error from the broker that kept its code and the broker's own
// ErrorMessage (which a response carries beside the code and which a franz-go
// producer discards). It unwraps to the *kerr.Error.
type kafkaRefusal struct {
	Err *kerr.Error
	Msg string
}

func (e *kafkaRefusal) Error() string {
	if e.Msg != "" {
		return e.Err.Message + ": " + e.Msg
	}
	return e.Err.Error()
}
func (e *kafkaRefusal) Unwrap() error { return e.Err }

// refusalFor sorts an error into a Refusal. It is the only place that decides.
func refusalFor(err error) Refusal {
	if err == nil {
		err = fmt.Errorf("%w: nothing to show", errNotFound)
	}
	var kr *kafkaRefusal
	var ke *kerr.Error
	var re *regError
	switch {
	case errors.As(err, &re):
		return registryRefusal(re, err)
	case errors.As(err, &kr):
		return kafkaRefusalFor(kr.Err, kr.Msg, err)
	case errors.As(err, &ke):
		return kafkaRefusalFor(ke, "", err)
	case errors.Is(err, errNotFound):
		return Refusal{Kind: Refused, Source: SourceConsole, Status: http.StatusNotFound, Heading: "Not found", NotFound: true,
			Message: err.Error(), Rule: "There is no such page or thing."}
	case errors.Is(err, errBadInput):
		return Refusal{Kind: Refused, Source: SourceConsole, Status: http.StatusBadRequest, Heading: "Check the form",
			Message: err.Error(), Rule: "Refused by the console: the request does not satisfy a rule of the form."}
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || isNetworkError(err):
		return Refusal{Kind: Failed, Source: SourceConsole, Status: http.StatusBadGateway, Heading: "Can't reach the broker", Retry: true,
			Message: "The console could not get an answer from the broker. It may have stopped, or be busy."}
	}
	return Refusal{Kind: Failed, Source: SourceConsole, Status: http.StatusInternalServerError, Heading: "Something went wrong", Retry: true, Message: err.Error()}
}

func kafkaRefusalFor(ke *kerr.Error, brokerMsg string, err error) Refusal {
	r := Refusal{Source: SourceKafka, Name: protocol.ErrorName(ke.Code), Code: int(ke.Code), Rule: ke.Description}
	say := brokerMsg
	if say == "" {
		say = strings.TrimSuffix(ke.Description, ".") + "."
	}
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.UnknownTopicID), errors.Is(err, kerr.GroupIDNotFound):
		r.Kind, r.Status, r.Heading, r.NotFound = Refused, http.StatusNotFound, "Not found", true
		r.Message = err.Error()
		if brokerMsg != "" {
			r.Message = brokerMsg
		}
		return r
	case ke.Code == kerr.SaslAuthenticationFailed.Code:
		r.Kind, r.Status, r.Heading = Denied, http.StatusBadGateway, "The broker refused the console's login"
		r.Message = "The console's own credentials were rejected by the broker."
		return r
	}
	// Which word it is comes from the table beside ErrorName, the one the Traffic page
	// classifies the wire's answers with.
	switch protocol.CodeClass(ke.Code) {
	case protocol.ClassDenied:
		r.Kind, r.Status, r.Heading = Denied, http.StatusForbidden, "Denied"
		r.Message = say
		return r
	case protocol.ClassNotImplemented:
		r.Kind, r.Status, r.Heading = NotImplemented, http.StatusNotImplemented, "Not implemented"
		r.Message = say
		return r
	case protocol.ClassFailed:
		// The broker's problem, not the request's: timeouts, a coordinator still
		// loading, a storage failure.
		r.Kind, r.Status, r.Heading, r.Retry = Failed, http.StatusServiceUnavailable, "The broker could not do that right now", true
		r.Message = strings.TrimSuffix(say, ".") + "."
		return r
	}
	r.Kind, r.Status, r.Heading = Refused, http.StatusBadRequest, "The broker refused that"
	r.Message = say
	return r
}

func registryRefusal(re *regError, err error) Refusal {
	r := Refusal{Source: SourceRegistry, Code: re.Code, Message: re.Message, Status: http.StatusBadRequest, Kind: Refused,
		Heading: "The schema registry refused that"}
	if re.Code != 0 {
		r.Name = strconv.Itoa(re.Code)
	}
	switch {
	case re.Status == http.StatusNotFound:
		r.Status, r.Heading, r.NotFound = http.StatusNotFound, "Not found", true
		r.Message = err.Error()
	case re.Status == http.StatusUnauthorized || re.Status == http.StatusForbidden:
		r.Kind, r.Status, r.Heading = Denied, http.StatusForbidden, "Denied"
	case re.Status == http.StatusNotImplemented:
		r.Kind, r.Status, r.Heading = NotImplemented, http.StatusNotImplemented, "Not implemented"
	case re.Status >= 500:
		r.Kind, r.Status, r.Heading, r.Retry = Failed, http.StatusBadGateway, "The schema registry failed", true
	case re.Status == http.StatusConflict || re.Status == http.StatusUnprocessableEntity:
		r.Rule = "The registry's own rule (its compatibility level or its mode) said no."
	}
	return r
}

// refusalData is a Refusal as template data.
func (r Refusal) data(extra map[string]any) map[string]any {
	d := map[string]any{"Refusal": r}
	for k, v := range extra {
		d[k] = v
	}
	return d
}
