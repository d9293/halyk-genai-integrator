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

type operationKey struct {
	operation domain.Operation
	version   int
}

// knownOperations lists every operation this build understands. Which of them
// accept tasks is decided by configuration (see NewUseCase).
var knownOperations = map[operationKey]operationSpec{
	{domain.OperationSocialMedia, 1}:     socialMediaV1{},
	{domain.OperationRegisterCompare, 1}: registerCompareV1{},
}

// SOCIAL_MEDIA v1 is a proposed contract, not yet confirmed with the AI team.
type socialMediaV1 struct{}

type socialMediaV1Input struct {
	IIN      *string `json:"iin"`
	FullName *string `json:"fullName"`
}

const maxFullNameLen = 500

var iinPattern = regexp.MustCompile(`^[0-9]{12}$`)

func (socialMediaV1) NormalizeInput(raw json.RawMessage) (json.RawMessage, []FieldError) {
	var in socialMediaV1Input
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

func (socialMediaV1) ValidateDocuments(docs []domain.Document) []FieldError {
	if len(docs) > 0 {
		return []FieldError{{Path: "documents", Code: CodeNotAllowed}}
	}
	return nil
}

func (socialMediaV1) RequiresDocuments() bool { return false }

// REGISTER_COMPARE v1 (Reestr): compare two documents, A and B.
type registerCompareV1 struct{}

type registerCompareV1Input struct {
	Adjudicate *bool `json:"adjudicate"`
}

func (registerCompareV1) NormalizeInput(raw json.RawMessage) (json.RawMessage, []FieldError) {
	var in registerCompareV1Input
	if fe := decodeInput(raw, &in); fe != nil {
		return nil, []FieldError{*fe}
	}
	if in.Adjudicate == nil {
		adjudicate := true
		in.Adjudicate = &adjudicate
	}
	return mustMarshal(in), nil
}

func (registerCompareV1) ValidateDocuments(docs []domain.Document) []FieldError {
	if len(docs) != 2 {
		return []FieldError{{Path: "documents", Code: CodeInvalidCount}}
	}
	// Two documents with unique role/ordinal (checked by the usecase), each A or B
	// with ordinal 1, means exactly one A and one B.
	var fields []FieldError
	for i, d := range docs {
		path := documentPath(i)
		if d.Role != "A" && d.Role != "B" {
			fields = append(fields, FieldError{Path: path + ".role", Code: CodeInvalidValue})
		}
		if d.Ordinal != 1 {
			fields = append(fields, FieldError{Path: path + ".ordinal", Code: CodeOutOfRange})
		}
		if d.SourceVersion == "" {
			fields = append(fields, FieldError{Path: path + ".sourceVersion", Code: CodeRequired})
		}
	}
	return fields
}

func (registerCompareV1) RequiresDocuments() bool { return true }

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
