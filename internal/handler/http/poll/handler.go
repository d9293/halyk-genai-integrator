// Package poll serves the internal API worker.Response uses to check the
// status of tasks at AI. The API decides which tasks are due for a check and
// applies the answers; the worker makes the actual call to AI.
package poll

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
)

const (
	codeAttemptNotFound    = "ATTEMPT_NOT_FOUND"
	codeClaimLost          = "CLAIM_LOST"
	messageInvalidRequest  = "Некорректный запрос"
	messageAttemptNotFound = "Попытка не найдена"
	messageClaimLost       = "Владение проверкой утрачено: токен неверен, срок истёк или ответ уже учтён"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out poll_service_mock_test.go . pollService

type pollService interface {
	Claim(ctx context.Context, workerID string, limit int) ([]poll.ClaimedPoll, error)
	Observe(ctx context.Context, attemptID, claimToken uuid.UUID, observation poll.Observation) error
}

type Handler struct {
	polls pollService
}

func NewHandler(polls pollService) *Handler {
	return &Handler{polls: polls}
}

func (h *Handler) Routes(rg *gin.RouterGroup) {
	rg.POST("/polls/claim", h.claim)
	rg.POST("/polls/:attemptId/observation", h.observation)
}

// claim godoc
//
//	@Summary		Claim status checks
//	@Description	For worker.Response. Hands out tasks at AI whose status is due to be checked. For each, make one GET to AI with attemptId as task_id and report the answer before claimUntil. Returns an empty list when no check is due; ask again later.
//	@Tags			polls (worker)
//	@Accept			json
//	@Produce		json
//	@Param			request	body		httpx.ClaimRequest	true	"Claim"
//	@Success		200		{object}	ClaimResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/polls/claim [post]
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

	claimed, err := h.polls.Claim(c.Request.Context(), workerID, limit)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}

	items := make([]ClaimedPoll, 0, len(claimed))
	for _, p := range claimed {
		items = append(items, toClaimedPoll(p))
	}
	c.JSON(http.StatusOK, ClaimResponse{Items: items})
}

// observation godoc
//
//	@Summary		Report what AI answered
//	@Description	For worker.Response. QUEUED / PROCESSING: checked again later. SUCCEEDED: the result is stored and the task succeeds. FAILED: the task fails. Both free the AI place. NOT_FOUND: AI does not know the task; several in a row mean AI lost it and the API sends it again. TECHNICAL_ERROR: checked again later, never treated as NOT_FOUND. Repeating QUEUED/PROCESSING/SUCCEEDED/FAILED after a lost response returns 200 again; repeating NOT_FOUND or TECHNICAL_ERROR returns 409.
//	@Tags			polls (worker)
//	@Accept			json
//	@Produce		json
//	@Param			attemptId	path		string				true	"attemptId from the claim response"	format(uuid)
//	@Param			request		body		ObservationRequest	true	"Observation"
//	@Success		200			{object}	httpx.AppliedResponse
//	@Failure		400			{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"ATTEMPT_NOT_FOUND"
//	@Failure		409			{object}	httpx.ErrorResponse	"CLAIM_LOST: wrong or expired claim token, or already applied"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/polls/{attemptId}/observation [post]
func (h *Handler) observation(c *gin.Context) {
	raw := c.Param("attemptId")
	attemptID, fe := httpx.ParseUUID("attemptId", &raw)
	if fe != nil {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, *fe)
		return
	}
	var req ObservationRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, observation, fields := req.validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	if err := h.polls.Observe(c.Request.Context(), attemptID, claimToken, observation); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

func (h *Handler) writeUsecaseError(c *gin.Context, err error) {
	switch {
	case httpx.ClientGone(c, err):
	case errors.Is(err, poll.ErrAttemptNotFound):
		httpx.WriteError(c, http.StatusNotFound, codeAttemptNotFound, messageAttemptNotFound)
	case errors.Is(err, poll.ErrClaimLost):
		httpx.WriteError(c, http.StatusConflict, codeClaimLost, messageClaimLost)
	case errors.Is(err, poll.ErrStorageUnavailable):
		slog.WarnContext(c.Request.Context(), "storage unavailable", "error", err, "path", c.FullPath())
		httpx.WriteServiceUnavailable(c)
	default:
		slog.ErrorContext(c.Request.Context(), "poll request failed", "error", err, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}
}
