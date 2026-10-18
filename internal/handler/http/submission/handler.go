// Package submission serves the internal API worker.Request uses to send
// ready tasks to AI. The API decides which task goes next and keeps the number
// of tasks at AI under the limit; the worker makes the actual call to AI.
package submission

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

const (
	codeAttemptNotFound    = "ATTEMPT_NOT_FOUND"
	codeClaimLost          = "CLAIM_LOST"
	messageInvalidRequest  = "Некорректный запрос"
	messageAttemptNotFound = "Попытка отправки не найдена"
	messageClaimLost       = "Владение попыткой утрачено: токен неверен или срок истёк"
)

//go:generate go run github.com/matryer/moq@v0.8.1 -out submission_service_mock_test.go . submissionService

type submissionService interface {
	Claim(ctx context.Context, workerID string, limit int) ([]submission.ClaimedSubmission, error)
	Report(ctx context.Context, attemptID, claimToken uuid.UUID, report submission.Report) error
	Renew(ctx context.Context, attemptID, claimToken uuid.UUID) (time.Time, error)
}

type Handler struct {
	submissions submissionService
}

func NewHandler(submissions submissionService) *Handler {
	return &Handler{submissions: submissions}
}

func (h *Handler) Routes(rg *gin.RouterGroup) {
	rg.POST("/submissions/claim", h.claim)
	rg.POST("/submissions/:attemptId/outcome", h.outcome)
	rg.POST("/submissions/:attemptId/renew", h.renew)
}

// claim godoc
//
//	@Summary		Claim tasks to send to AI
//	@Description	For worker.Request. Hands out tasks that are ready to be sent to AI, each as a new attempt that already holds one of the limited AI places. Send it to AI with attemptId as task_id, then report the outcome. Returns an empty list when nothing is ready or all AI places are taken; ask again later.
//	@Tags			submissions (worker)
//	@Accept			json
//	@Produce		json
//	@Param			request	body		httpx.ClaimRequest	true	"Claim"
//	@Success		200		{object}	ClaimResponse
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/submissions/claim [post]
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

	claimed, err := h.submissions.Claim(c.Request.Context(), workerID, limit)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}

	items := make([]ClaimedSubmission, 0, len(claimed))
	for _, s := range claimed {
		items = append(items, toClaimedSubmission(s))
	}
	c.JSON(http.StatusOK, ClaimResponse{Items: items})
}

// outcome godoc
//
//	@Summary		Report what happened when sending to AI
//	@Description	For worker.Request. ACCEPTED: AI took the task; it keeps its AI place and will be polled. UNKNOWN: no reliable answer; AI may have the task, so it keeps its place and polling finds out. Never re-send it yourself. REJECTED: AI clearly refused; the place is freed and the task fails.
//	@Tags			submissions (worker)
//	@Accept			json
//	@Produce		json
//	@Param			attemptId	path		string			true	"attemptId from the claim response"	format(uuid)
//	@Param			request		body		OutcomeRequest	true	"Outcome"
//	@Success		200			{object}	httpx.AppliedResponse
//	@Failure		400			{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"ATTEMPT_NOT_FOUND"
//	@Failure		409			{object}	httpx.ErrorResponse	"CLAIM_LOST: wrong or expired claim token"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/submissions/{attemptId}/outcome [post]
func (h *Handler) outcome(c *gin.Context) {
	attemptID, ok := attemptIDParam(c)
	if !ok {
		return
	}
	var req OutcomeRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}
	claimToken, report, fields := req.validate()
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	if err := h.submissions.Report(c.Request.Context(), attemptID, claimToken, report); err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.AppliedResponse{Applied: true})
}

// renew godoc
//
//	@Summary		Extend a submission claim
//	@Description	For worker.Request. Heartbeat while the call to AI is in progress: extends claimUntil. On 409 stop: the claim expired and the attempt is being treated as UNKNOWN.
//	@Tags			submissions (worker)
//	@Accept			json
//	@Produce		json
//	@Param			attemptId	path		string				true	"attemptId from the claim response"	format(uuid)
//	@Param			request		body		httpx.RenewRequest	true	"Claim to extend"
//	@Success		200			{object}	httpx.RenewResponse
//	@Failure		400			{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"ATTEMPT_NOT_FOUND"
//	@Failure		409			{object}	httpx.ErrorResponse	"CLAIM_LOST"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/internal/v1/submissions/{attemptId}/renew [post]
func (h *Handler) renew(c *gin.Context) {
	attemptID, ok := attemptIDParam(c)
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

	claimUntil, err := h.submissions.Renew(c.Request.Context(), attemptID, claimToken)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.RenewResponse{ClaimUntil: claimUntil.UTC()})
}

func attemptIDParam(c *gin.Context) (uuid.UUID, bool) {
	raw := c.Param("attemptId")
	id, fe := httpx.ParseUUID("attemptId", &raw)
	if fe != nil {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, *fe)
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) writeUsecaseError(c *gin.Context, err error) {
	switch {
	case httpx.ClientGone(c, err):
	case errors.Is(err, submission.ErrAttemptNotFound):
		httpx.WriteError(c, http.StatusNotFound, codeAttemptNotFound, messageAttemptNotFound)
	case errors.Is(err, submission.ErrClaimLost):
		httpx.WriteError(c, http.StatusConflict, codeClaimLost, messageClaimLost)
	case errors.Is(err, submission.ErrStorageUnavailable):
		slog.WarnContext(c.Request.Context(), "storage unavailable", "error", err, "path", c.FullPath())
		httpx.WriteServiceUnavailable(c)
	default:
		slog.ErrorContext(c.Request.Context(), "submission request failed", "error", err, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}
}
