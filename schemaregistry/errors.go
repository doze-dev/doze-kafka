package schemaregistry

import "fmt"

// Error is a registry failure as Confluent's REST API reports it: an HTTP status and
// a registry error code in the body. The codes are Confluent's, because client
// libraries and tools key on them (40401 means "no such subject" to every serializer).
type Error struct {
	Status  int    `json:"-"`
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Code, e.Message) }

const (
	codeSubjectNotFound       = 40401
	codeVersionNotFound       = 40402
	codeSchemaNotFound        = 40403
	codeSubjectSoftDeleted    = 40404
	codeSubjectNotSoftDeleted = 40405
	codeVersionSoftDeleted    = 40406
	codeVersionNotSoftDeleted = 40407
	codeSubjectCompatNotSet   = 40408
	codeSubjectModeNotSet     = 40409
	codeIncompatible          = 409
	codeInvalidSchema         = 42201
	codeInvalidVersion        = 42202
	codeInvalidCompatibility  = 42203
	codeInvalidMode           = 42204
	codeOperationNotPermitted = 42205
	codeReferenceExists       = 42206
	codeInvalidRequest        = 42207
	codeStore                 = 50001
	codeInternal              = 500
	codeUnauthorized          = 401
	codeForbidden             = 403
	codeNotFound              = 404
	codeMethodNotAllowed      = 405
	codeUnprocessable         = 422
	codeUnsupportedMediaType  = 415
)

func newErr(status, code int, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func errSubjectNotFound(s string) *Error {
	return newErr(404, codeSubjectNotFound, "Subject '%s' not found.", s)
}
func errVersionNotFound(s string, v int) *Error {
	return newErr(404, codeVersionNotFound, "Version %d not found.", v)
}
func errSchemaNotFound() *Error { return newErr(404, codeSchemaNotFound, "Schema not found") }
func errSchemaIDNotFound(id int) *Error {
	return newErr(404, codeSchemaNotFound, "Schema %d not found", id)
}
func errInvalidSchema(format string, args ...any) *Error {
	return newErr(422, codeInvalidSchema, format, args...)
}
func errInvalidVersion(v string) *Error {
	return newErr(422, codeInvalidVersion, "The specified version '%s' is not a valid version id. Allowed values are between [1, 2^31-1] and the string \"latest\"", v)
}
