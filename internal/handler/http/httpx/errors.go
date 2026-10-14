// Package httpx holds HTTP helpers shared by the handler packages.
package httpx

import (
	"github.com/gin-gonic/gin"
)

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
	// TraceID is null until tracing is wired in.
	TraceID *string `json:"traceId" extensions:"x-nullable"`
}

type ErrorDetail struct {
	Code    string `json:"code" example:"VALIDATION_ERROR"`
	Message string `json:"message" example:"Некорректный запрос"`
	// Retryable means the same HTTP command may be repeated, not that AI work may be restarted.
	Retryable bool         `json:"retryable" example:"false"`
	Fields    []FieldError `json:"fields,omitempty"`
}

type FieldError struct {
	Path string `json:"path" example:"input.iin"`
	Code string `json:"code" example:"INVALID_FORMAT"`
}

// Error codes shared by all handlers.
const (
	CodeValidationError  = "VALIDATION_ERROR"
	CodeMalformedJSON    = "MALFORMED_JSON"
	CodeBodyTooLarge     = "BODY_TOO_LARGE"
	CodeNotFound         = "NOT_FOUND"
	CodeMethodNotAllowed = "METHOD_NOT_ALLOWED"
	CodeInternalError    = "INTERNAL_ERROR"
)

// Field-level error codes.
const (
	FieldRequired      = "REQUIRED"
	FieldInvalidFormat = "INVALID_FORMAT"
	FieldInvalidType   = "INVALID_TYPE"
	FieldUnknown       = "UNKNOWN_FIELD"
	FieldDuplicateKey  = "DUPLICATE_KEY"
	FieldTooLong       = "TOO_LONG"
	FieldOutOfRange    = "OUT_OF_RANGE"
)

func WriteError(c *gin.Context, status int, code, message string, fields ...FieldError) {
	c.AbortWithStatusJSON(status, ErrorResponse{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
			Fields:  fields,
		},
	})
}
