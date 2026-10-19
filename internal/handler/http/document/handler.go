// Package document serves the internal API worker.Request uses to copy task
// documents from Attstore to S3. The API only hands out the work and records
// the outcome; the worker does the copying.
package document

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/document"
)

const (
	codeDocumentNotFound    = "DOCUMENT_NOT_FOUND"
	codeClaimLost           = "CLAIM_LOST"
	messageInvalidRequest   = "Некорректный запрос"
	messageDocumentNotFound = "Документ не найден"
	messageClaimLost        = "Владение документом утрачено: токен неверен или срок истёк"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out document_service_mock_test.go . documentService

type documentService interface {
	Claim(ctx context.Context, workerID string, limit int) ([]document.ClaimedDocument, error)
	Complete(ctx context.Context, id, claimToken uuid.UUID, result document.CopyResult) error
	Fail(ctx context.Context, id, claimToken uuid.UUID, failure document.CopyFailure) error
	Renew(ctx context.Context, id, claimToken uuid.UUID) (time.Time, error)
}

type Handler struct {
	documents documentService
}

func NewHandler(documents documentService) *Handler {
	return &Handler{documents: documents}
}

func (h *Handler) Routes(rg *gin.RouterGroup) {
	rg.POST("/documents/claim", h.claim)
	rg.POST("/documents/:id/complete", h.complete)
	rg.POST("/documents/:id/failure", h.failure)
	rg.POST("/documents/:id/renew", h.renew)
}

// claim godoc
//
//	@Summary		Claim documents to copy
//	@Description	For worker.Request. Hands out documents that must be copied from Attstore to S3 before the task can be sent to AI. Each document is owned by the caller until claimUntil; extend it with /documents/{id}/renew while copying. Returns an empty list when there is nothing to copy.
//	@Tags			documents (worker)
//	@Accept			json
//	@Produce		json
//	@Param			request	body		httpx.ClaimRequest	true	"Claim"
//	@Success		200		{object}	ClaimResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/documents/claim [post]
func (h *Handler) claim(c *gin.Context) {
	var req httpx.ClaimRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	workerID, limit, fields := req.Validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	claimed, err := h.documents.Claim(c.Request.Context(), workerID, limit)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}

	items := make([]ClaimedDocument, 0, len(claimed))
	for _, d := range claimed {
		items = append(items, toClaimedDocument(d))
	}
	c.JSON(http.StatusOK, ClaimResponse{Items: items})
}

// complete godoc
//
//	@Summary		Report a copied document
//	@Description	For worker.Request. The document was copied to S3. When it is the task's last document, the task becomes ready to be sent to AI. Repeating the call with the same data returns 200 again.
//	@Tags			documents (worker)
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string			true	"id from the claim response (not bpmDocumentId)"	format(uuid)
//	@Param			request	body		CompleteRequest	true	"Copy result"
//	@Success		200		{object}	httpx.AppliedResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404		{object}	httpx.ErrorResponse	"DOCUMENT_NOT_FOUND"
//	@Failure		409		{object}	httpx.ErrorResponse	"CLAIM_LOST: wrong or expired claim token; drop the work"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/documents/{id}/complete [post]
func (h *Handler) complete(c *gin.Context) {
	id, ok := documentID(c)
	if !ok {
		return
	}
	var req CompleteRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, result, fields := req.validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	if err := h.documents.Complete(c.Request.Context(), id, claimToken, result); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

// failure godoc
//
//	@Summary		Report a failed copy
//	@Description	For worker.Request. The document could not be copied. A retryable failure is tried again later with backoff; a permanent one, or too many failures, puts the document and its task into NEEDS_ATTENTION.
//	@Tags			documents (worker)
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string			true	"id from the claim response (not bpmDocumentId)"	format(uuid)
//	@Param			request	body		FailureRequest	true	"Failure"
//	@Success		200		{object}	httpx.AppliedResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404		{object}	httpx.ErrorResponse	"DOCUMENT_NOT_FOUND"
//	@Failure		409		{object}	httpx.ErrorResponse	"CLAIM_LOST: wrong or expired claim token; drop the work"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/documents/{id}/failure [post]
func (h *Handler) failure(c *gin.Context) {
	id, ok := documentID(c)
	if !ok {
		return
	}
	var req FailureRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, failure, fields := req.validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	if err := h.documents.Fail(c.Request.Context(), id, claimToken, failure); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

// renew godoc
//
//	@Summary		Extend a document claim
//	@Description	For worker.Request. Heartbeat while copying: extends claimUntil. Call it about every quarter of the claim time. On 409 stop copying: the claim expired or belongs to someone else.
//	@Tags			documents (worker)
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string			true	"id from the claim response (not bpmDocumentId)"	format(uuid)
//	@Param			request	body		httpx.RenewRequest	true	"Claim to extend"
//	@Success		200		{object}	httpx.RenewResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404		{object}	httpx.ErrorResponse	"DOCUMENT_NOT_FOUND"
//	@Failure		409		{object}	httpx.ErrorResponse	"CLAIM_LOST"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/documents/{id}/renew [post]
func (h *Handler) renew(c *gin.Context) {
	id, ok := documentID(c)
	if !ok {
		return
	}
	var req httpx.RenewRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, fields := req.Validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	claimUntil, err := h.documents.Renew(c.Request.Context(), id, claimToken)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.RenewResponse{ClaimUntil: claimUntil.UTC()})
}

func documentID(c *gin.Context) (uuid.UUID, bool) {
	raw := c.Param("id")
	id, fe := httpx.ParseUUID("id", &raw)
	if fe != nil {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, *fe)
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) writeUsecaseError(c *gin.Context, err error) {
	switch {
	case httpx.ClientGone(c, err):
	case errors.Is(err, document.ErrDocumentNotFound):
		httpx.WriteError(c, http.StatusNotFound, codeDocumentNotFound, messageDocumentNotFound)
	case errors.Is(err, document.ErrClaimLost):
		httpx.WriteError(c, http.StatusConflict, codeClaimLost, messageClaimLost)
	case errors.Is(err, document.ErrStorageUnavailable):
		slog.WarnContext(c.Request.Context(), "storage unavailable", "error", err, "path", c.FullPath())
		httpx.WriteServiceUnavailable(c)
	default:
		slog.ErrorContext(c.Request.Context(), "document request failed", "error", err, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}
}
