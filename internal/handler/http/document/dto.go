package document

import (
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/document"
)

const (
	maxURILen         = 8192
	maxFailureCodeLen = 100
	maxMessageLen     = 2000
)

var sha256Hex = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

type ClaimResponse struct {
	// Empty when there is nothing to copy.
	Items []ClaimedDocument `json:"items"`
}

type ClaimedDocument struct {
	// Use this id in /documents/{id}/complete, /failure and /renew.
	ID          uuid.UUID `json:"id" format:"uuid" example:"286a795c-37bf-4ea3-9c85-47c886cf3f2a"`
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"`
	// Operation of the task the document belongs to, e.g. for the storage path.
	Operation string `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"REGISTER_COMPARE"`
	// BPM's ID of the file, for logging only. Not accepted in the URLs.
	BPMDocumentID uuid.UUID `json:"bpmDocumentId" format:"uuid" example:"22222222-2222-4222-8222-222222222222"`
	// Where to download the file from.
	AttstoreURL string `json:"attstoreUrl" example:"https://attstore.example/documents/1"`
	// Index of the file in the task; AI receives files in this order.
	Position int `json:"position" example:"0"`
	// Proof of ownership; send it back with every call about this document.
	ClaimToken uuid.UUID `json:"claimToken" format:"uuid" example:"7b1e2c3d-0000-4000-8000-000000000001"`
	// Ownership expires at this time unless renewed.
	ClaimUntil time.Time `json:"claimUntil" example:"2026-10-15T08:01:00Z"`
}

type CompleteRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
	// Link to the copy in S3 that AI will receive.
	S3URI          *string `json:"s3Uri" maxLength:"8192" example:"s3://ai-input/tasks/aaaaaaaa/0" validate:"required"`
	SizeBytes      *int64  `json:"sizeBytes" minimum:"0" example:"123456" validate:"required"`
	ChecksumSHA256 *string `json:"checksumSha256" example:"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" validate:"required"`
}

func (r CompleteRequest) validate() (claimToken uuid.UUID, result document.CopyResult, fields []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}

	switch {
	case r.S3URI == nil || *r.S3URI == "":
		fields = append(fields, httpx.FieldError{Path: "s3Uri", Code: httpx.FieldRequired})
	case !strings.HasPrefix(*r.S3URI, "s3://"):
		fields = append(fields, httpx.FieldError{Path: "s3Uri", Code: httpx.FieldInvalidFormat})
	case len(*r.S3URI) > maxURILen:
		fields = append(fields, httpx.FieldError{Path: "s3Uri", Code: httpx.FieldTooLong})
	default:
		result.S3URI = *r.S3URI
	}

	switch {
	case r.SizeBytes == nil:
		fields = append(fields, httpx.FieldError{Path: "sizeBytes", Code: httpx.FieldRequired})
	case *r.SizeBytes < 0:
		fields = append(fields, httpx.FieldError{Path: "sizeBytes", Code: httpx.FieldOutOfRange})
	default:
		result.SizeBytes = *r.SizeBytes
	}

	switch {
	case r.ChecksumSHA256 == nil || *r.ChecksumSHA256 == "":
		fields = append(fields, httpx.FieldError{Path: "checksumSha256", Code: httpx.FieldRequired})
	case !sha256Hex.MatchString(*r.ChecksumSHA256):
		fields = append(fields, httpx.FieldError{Path: "checksumSha256", Code: httpx.FieldInvalidFormat})
	default:
		result.ChecksumSHA256 = strings.ToLower(*r.ChecksumSHA256)
	}
	return claimToken, result, fields
}

type FailureRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
	// Short machine-readable reason.
	Code *string `json:"code" maxLength:"100" example:"SOURCE_UNAVAILABLE" validate:"required"`
	// Human-readable details. No personal data or signed URLs.
	Message *string `json:"message" maxLength:"2000" example:"Attstore returned 503" validate:"required"`
	// Whether trying again later may help. The API applies its own retry limit.
	Retryable *bool `json:"retryable" example:"true" validate:"required"`
}

func (r FailureRequest) validate() (claimToken uuid.UUID, failure document.CopyFailure, fields []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}

	switch {
	case r.Code == nil || strings.TrimSpace(*r.Code) == "":
		fields = append(fields, httpx.FieldError{Path: "code", Code: httpx.FieldRequired})
	case len(*r.Code) > maxFailureCodeLen:
		fields = append(fields, httpx.FieldError{Path: "code", Code: httpx.FieldTooLong})
	default:
		failure.Code = *r.Code
	}

	switch {
	case r.Message == nil || strings.TrimSpace(*r.Message) == "":
		fields = append(fields, httpx.FieldError{Path: "message", Code: httpx.FieldRequired})
	case len([]rune(*r.Message)) > maxMessageLen:
		fields = append(fields, httpx.FieldError{Path: "message", Code: httpx.FieldTooLong})
	default:
		failure.Message = *r.Message
	}

	if r.Retryable == nil {
		fields = append(fields, httpx.FieldError{Path: "retryable", Code: httpx.FieldRequired})
	} else {
		failure.Retryable = *r.Retryable
	}
	return claimToken, failure, fields
}

func toClaimedDocument(d document.ClaimedDocument) ClaimedDocument {
	return ClaimedDocument{
		ID:            d.ID,
		RequestGUID:   d.RequestGUID,
		Operation:     string(d.Operation),
		BPMDocumentID: d.DocumentID,
		AttstoreURL:   d.AttstoreURL,
		Position:      d.Position,
		ClaimToken:    d.ClaimToken,
		ClaimUntil:    d.ClaimUntil.UTC(),
	}
}
