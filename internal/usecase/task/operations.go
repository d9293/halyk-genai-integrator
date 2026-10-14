package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

// operationSpec is the per-operation strategy. Adding an operation means
// adding a spec to knownOperations, not touching the handler or usecase.
type operationSpec interface {
	// NormalizeInput validates the input object and returns its canonical JSON,
	// with defaults materialized, so equal requests hash equally.
	NormalizeInput(raw json.RawMessage) (json.RawMessage, []FieldError)
	// ValidateDocuments checks the operation's document rules.
	ValidateDocuments(docs []domain.Document) []FieldError
	RequiresDocuments() bool
}

// knownOperations lists every operation this build understands. Which of them
// accept tasks is decided by configuration (see NewUseCase).
var knownOperations = map[domain.Operation]operationSpec{
	domain.OperationSocialMedia:     socialMedia{},
	domain.OperationRegisterCompare: registerCompare{},
}

// SOCIAL_MEDIA is a proposed contract, not yet confirmed with the AI team.
type socialMedia struct{}

type socialMediaInput struct {
	IIN      *string `json:"iin"`
	FullName *string `json:"fullName"`
}

const maxFullNameLen = 500

var iinPattern = regexp.MustCompile(`^[0-9]{12}$`)

func (socialMedia) NormalizeInput(raw json.RawMessage) (json.RawMessage, []FieldError) {
	var in socialMediaInput
	if fe := decodeInput(raw, &in); fe != nil {
		return nil, []FieldError{*fe}
	}

	var fields []FieldError
	switch {
	case in.IIN == nil:
		fields = append(fields, FieldError{Path: "input.iin", Code: CodeRequired})
	case !iinPattern.MatchString(*in.IIN):
		fields = append(fields, FieldError{Path: "input.iin", Code: CodeInvalidFormat})
	}
	switch {
	case in.FullName == nil || strings.TrimSpace(*in.FullName) == "":
		fields = append(fields, FieldError{Path: "input.fullName", Code: CodeRequired})
	case utf8.RuneCountInString(*in.FullName) > maxFullNameLen:
		fields = append(fields, FieldError{Path: "input.fullName", Code: CodeTooLong})
	}
	if len(fields) > 0 {
		return nil, fields
	}
	return mustMarshal(in), nil
}

func (socialMedia) ValidateDocuments(docs []domain.Document) []FieldError {
	if len(docs) > 0 {
		return []FieldError{{Path: "documents", Code: CodeNotAllowed}}
	}
	return nil
}

func (socialMedia) RequiresDocuments() bool { return false }

// REGISTER_COMPARE (Reestr): compare the people listed in two documents.
type registerCompare struct{}

type registerCompareInput struct {
	Adjudicate *bool `json:"adjudicate"`
}

func (registerCompare) NormalizeInput(raw json.RawMessage) (json.RawMessage, []FieldError) {
	var in registerCompareInput
	if fe := decodeInput(raw, &in); fe != nil {
		return nil, []FieldError{*fe}
	}
	if in.Adjudicate == nil {
		adjudicate := true
		in.Adjudicate = &adjudicate
	}
	return mustMarshal(in), nil
}

func (registerCompare) ValidateDocuments(docs []domain.Document) []FieldError {
	if len(docs) != 2 {
		return []FieldError{{Path: "documents", Code: CodeInvalidCount}}
	}
	return nil
}

func (registerCompare) RequiresDocuments() bool { return true }

// decodeInput strictly decodes an operation input. Duplicate keys were already
// rejected by the HTTP layer.
func decodeInput(raw json.RawMessage, dst any) *FieldError {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil {
		return nil
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return &FieldError{Path: "input." + typeErr.Field, Code: CodeInvalidType}
	}
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return &FieldError{Path: "input." + strings.Trim(name, `"`), Code: CodeUnknownField}
	}
	return &FieldError{Path: "input", Code: CodeInvalidFormat}
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only plain structs are passed here
	}
	return b
}
