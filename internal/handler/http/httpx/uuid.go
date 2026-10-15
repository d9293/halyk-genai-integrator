package httpx

import (
	"regexp"

	"github.com/google/uuid"
)

// canonicalUUID accepts only the 8-4-4-4-12 form; uuid.Parse alone also
// accepts braces, urn: prefixes and undashed hex.
var canonicalUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ParseUUID parses a required UUID field, reporting problems under path.
func ParseUUID(path string, s *string) (uuid.UUID, *FieldError) {
	if s == nil || *s == "" {
		return uuid.Nil, &FieldError{Path: path, Code: FieldRequired}
	}
	if !canonicalUUID.MatchString(*s) {
		return uuid.Nil, &FieldError{Path: path, Code: FieldInvalidFormat}
	}
	return uuid.MustParse(*s), nil
}
