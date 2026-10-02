package meta

import (
	"errors"
	"fmt"
)

// ErrInvalidTopicName is returned (wrapped) for a name Kafka would reject.
var ErrInvalidTopicName = errors.New("meta: invalid topic name")

// MaxTopicNameLen is Kafka's limit on a topic name's length.
const MaxTopicNameLen = 249

// ValidateTopicName applies Kafka's rules for a new topic: 1–249 characters from
// [a-zA-Z0-9._-], and not "." or "..".
//
// This is a security boundary as well as a compatibility one. A topic's name
// becomes a directory name under the data dir, so a name like "../victim" would
// create — and later delete — a directory outside it. Kafka's alphabet has no
// path separator in it, which is exactly why enforcing it closes that hole.
//
// Only creation is validated. Topics already in the registry keep working even
// if their names predate this check; the storage layer separately refuses any
// name that could escape the data dir (see log.ErrUnsafeTopicName).
func ValidateTopicName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: name is empty", ErrInvalidTopicName)
	case name == "." || name == "..":
		return fmt.Errorf("%w: %q is not allowed", ErrInvalidTopicName, name)
	case len(name) > MaxTopicNameLen:
		return fmt.Errorf("%w: %d characters is longer than the maximum of %d", ErrInvalidTopicName, len(name), MaxTopicNameLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		if !ok {
			return fmt.Errorf("%w: %q contains %q; only ASCII letters, digits, '.', '_' and '-' are allowed", ErrInvalidTopicName, name, string(c))
		}
	}
	return nil
}
