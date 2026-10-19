// Package result serves the internal API worker.Response uses to deliver
// final task results to BPM through Kafka. The API hands out the ready
// messages and records whether Kafka accepted them; the worker publishes.
package result

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/result"
)

const (
	codeResultNotFound    = "RESULT_NOT_FOUND"
	codeClaimLost         = "CLAIM_LOST"
	messageInvalidRequest = "Некорректный запрос"
	messageNotFound       = "Результат не найден"
	messageClaimLost      = "Владение результатом утрачено: токен неверен или срок истёк"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out result_service_mock_test.go . resultService

type resultService interface {
	Claim(ctx context.Context, workerID string, limit int) ([]result.ClaimedResult, error)
	Published(ctx context.Context, requestGUID, claimToken uuid.UUID) error
	Failed(ctx context.Context, requestGUID, claimToken uuid.UUID, failure domain.Failure) error
}

type Handler struct {
	results resultService
}

func NewHandler(results resultService) *Handler {
	return &Handler{results: results}
}

func (h *Handler) Routes(rg *gin.RouterGroup) {
	rg.POST("/results/claim", h.claim)
	rg.POST("/results/:requestGuid/published", h.published)
	rg.POST("/results/:requestGuid/failure", h.failure)
}

// claim godoc
//
//	@Summary		Claim results to deliver
//	@Description	For worker.Response. Hands out final task results (SUCCEEDED / FAILED) that BPM has not received yet. Publish each message unchanged to the given topic with the given key, wait for the broker ACK, then report published or failure. Returns an empty list when nothing is waiting.
//	@Tags			results (worker)
//	@Accept			json
//	@Produce		json
//	@Param			request	body		httpx.ClaimRequest	true	"Claim"
//	@Success		200		{object}	ClaimResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/results/claim [post]
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

	claimed, err := h.results.Claim(c.Request.Context(), workerID, limit)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}

	items := make([]ClaimedResult, 0, len(claimed))
	for _, r := range claimed {
		items = append(items, toClaimedResult(r))
	}
	c.JSON(http.StatusOK, ClaimResponse{Items: items})
}

// published godoc
//
//	@Summary		Report a result delivered to Kafka
//	@Description	For worker.Response. Call only after the broker ACK. The result is not sent again. Repeating the call returns 200 again.
//	@Tags			results (worker)
//	@Accept			json
//	@Produce		json
//	@Param			requestGuid	path		string				true	"requestGuid from the claim response"	format(uuid)
//	@Param			request		body		PublishedRequest	true	"Claim token"
//	@Success		200			{object}	httpx.AppliedResponse
//	@Failure		400			{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"RESULT_NOT_FOUND"
//	@Failure		409			{object}	httpx.ErrorResponse	"CLAIM_LOST: wrong or expired claim token; the result may be published again, BPM deduplicates by requestGuid"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/results/{requestGuid}/published [post]
func (h *Handler) published(c *gin.Context) {
	requestGUID, ok := requestGUIDParam(c)
	if !ok {
		return
	}
	var req PublishedRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, fields := req.validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	if err := h.results.Published(c.Request.Context(), requestGUID, claimToken); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

// failure godoc
//
//	@Summary		Report a result Kafka did not accept
//	@Description	For worker.Response. The result is tried again later with backoff; it is never dropped.
//	@Tags			results (worker)
//	@Accept			json
//	@Produce		json
//	@Param			requestGuid	path		string			true	"requestGuid from the claim response"	format(uuid)
//	@Param			request		body		FailureRequest	true	"Failure"
//	@Success		200			{object}	httpx.AppliedResponse
//	@Failure		400			{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"RESULT_NOT_FOUND"
//	@Failure		409			{object}	httpx.ErrorResponse	"CLAIM_LOST"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/results/{requestGuid}/failure [post]
func (h *Handler) failure(c *gin.Context) {
	requestGUID, ok := requestGUIDParam(c)
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

	if err := h.results.Failed(c.Request.Context(), requestGUID, claimToken, failure); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

func requestGUIDParam(c *gin.Context) (uuid.UUID, bool) {
	raw := c.Param("requestGuid")
	id, fe := httpx.ParseUUID("requestGuid", &raw)
	if fe != nil {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, *fe)
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) writeUsecaseError(c *gin.Context, err error) {
	switch {
	case httpx.ClientGone(c, err):
	case errors.Is(err, result.ErrResultNotFound):
		httpx.WriteError(c, http.StatusNotFound, codeResultNotFound, messageNotFound)
	case errors.Is(err, result.ErrClaimLost):
		httpx.WriteError(c, http.StatusConflict, codeClaimLost, messageClaimLost)
	case errors.Is(err, result.ErrStorageUnavailable):
		slog.WarnContext(c.Request.Context(), "storage unavailable", "error", err, "path", c.FullPath())
		httpx.WriteServiceUnavailable(c)
	default:
		slog.ErrorContext(c.Request.Context(), "result request failed", "error", err, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}
}
