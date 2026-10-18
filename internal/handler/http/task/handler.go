package task

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

const (
	codeTaskNotFound           = "TASK_NOT_FOUND"
	codeIdempotencyConflict    = "IDEMPOTENCY_CONFLICT"
	codeRequestGUIDUnavailable = "REQUEST_GUID_UNAVAILABLE"
	codeOperationNotEnabled    = "OPERATION_NOT_ENABLED"
	fieldCodeNotEnabled        = "NOT_ENABLED"
	messageInvalidRequest      = "Некорректный запрос"
	messageTaskNotFound        = "Задача не найдена"
	messageIdempotency         = "Задача с этим requestGuid уже зарегистрирована с другими параметрами"
	messageRequestGUIDTaken    = "requestGuid недоступен"
	messageOperationNotEnabled = "Операция не поддерживается"
)

//go:generate go run github.com/matryer/moq@v0.8.1 -out task_service_mock_test.go . taskService

type taskService interface {
	// Register returns the stored task and whether this call created it.
	Register(ctx context.Context, in task.RegisterInput) (domain.Task, bool, error)
	Get(ctx context.Context, callerID string, requestGUID uuid.UUID) (domain.Task, error)
}

type Handler struct {
	tasks taskService
}

func NewHandler(tasks taskService) *Handler {
	return &Handler{tasks: tasks}
}

func (h *Handler) Routes(rg *gin.RouterGroup) {
	rg.POST("/tasks", h.create)
	rg.GET("/tasks/:requestGuid", h.get)
}

// create godoc
//
//	@Summary		Register task
//	@Description	Сохраняет задачу BPM и возвращает 202 после commit. Повтор с тем же requestGuid и теми же данными возвращает 200 и текущее состояние; другой вход — 409 IDEMPOTENCY_CONFLICT.
//	@Tags			tasks
//	@Accept			json
//	@Produce		json
//	@Param			request	body		CreateTaskRequest	true	"Task"
//	@Success		202		{object}	CreateTaskResponse	"Task registered"
//	@Header			202		{string}	Location			"/api/v1/tasks/{requestGuid}"
//	@Success		200		{object}	CreateTaskResponse	"Same task already registered"
//	@Failure		400		{object}	httpx.ErrorResponse	"MALFORMED_JSON, VALIDATION_ERROR"
//	@Failure		409		{object}	httpx.ErrorResponse	"IDEMPOTENCY_CONFLICT, REQUEST_GUID_UNAVAILABLE"
//	@Failure		413		{object}	httpx.ErrorResponse	"BODY_TOO_LARGE"
//	@Failure		422		{object}	httpx.ErrorResponse	"OPERATION_NOT_ENABLED, VALIDATION_ERROR"
//	@Failure		500		{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503		{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE: database unreachable, retry with the same body"
//	@Router			/api/v1/tasks [post]
func (h *Handler) create(c *gin.Context) {
	var req CreateTaskRequest
	if !httpx.DecodeJSON(c, &req) {
		return
	}

	in, fields := req.toInput(httpx.CallerID(c))
	if len(fields) > 0 {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, fields...)
		return
	}

	t, created, err := h.tasks.Register(c.Request.Context(), in)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}

	if !created {
		c.JSON(http.StatusOK, toCreateResponse(t))
		return
	}
	c.Header("Location", "/api/v1/tasks/"+t.RequestGUID.String())
	c.JSON(http.StatusAccepted, toCreateResponse(t))
}

// get godoc
//
//	@Summary		Get task
//	@Description	Текущее состояние задачи, результат и ссылки на отчёты.
//	@Tags			tasks
//	@Produce		json
//	@Param			requestGuid	path		string	true	"Request GUID"	format(uuid)
//	@Success		200			{object}	TaskView
//	@Failure		400			{object}	httpx.ErrorResponse	"VALIDATION_ERROR"
//	@Failure		404			{object}	httpx.ErrorResponse	"TASK_NOT_FOUND"
//	@Failure		500			{object}	httpx.ErrorResponse	"INTERNAL_ERROR"
//	@Failure		503			{object}	httpx.ErrorResponse	"SERVICE_UNAVAILABLE"
//	@Router			/api/v1/tasks/{requestGuid} [get]
func (h *Handler) get(c *gin.Context) {
	raw := c.Param("requestGuid")
	requestGUID, fe := httpx.ParseUUID("requestGuid", &raw)
	if fe != nil {
		httpx.WriteError(c, http.StatusBadRequest, httpx.CodeValidationError, messageInvalidRequest, *fe)
		return
	}

	t, err := h.tasks.Get(c.Request.Context(), httpx.CallerID(c), requestGUID)
	if err != nil {
		h.writeUsecaseError(c, err)
		return
	}
	c.JSON(http.StatusOK, toTaskView(t))
}

func (h *Handler) writeUsecaseError(c *gin.Context, err error) {
	var validationErr *task.ValidationError
	switch {
	case httpx.ClientGone(c, err):
	case errors.As(err, &validationErr):
		fields := make([]httpx.FieldError, 0, len(validationErr.Fields))
		for _, f := range validationErr.Fields {
			fields = append(fields, httpx.FieldError{Path: f.Path, Code: f.Code})
		}
		httpx.WriteError(c, http.StatusUnprocessableEntity, httpx.CodeValidationError, messageInvalidRequest, fields...)
	case errors.Is(err, task.ErrOperationNotEnabled):
		httpx.WriteError(c, http.StatusUnprocessableEntity, codeOperationNotEnabled, messageOperationNotEnabled,
			httpx.FieldError{Path: "operation", Code: fieldCodeNotEnabled})
	case errors.Is(err, task.ErrTaskNotFound):
		httpx.WriteError(c, http.StatusNotFound, codeTaskNotFound, messageTaskNotFound)
	case errors.Is(err, task.ErrIdempotencyConflict):
		httpx.WriteError(c, http.StatusConflict, codeIdempotencyConflict, messageIdempotency)
	case errors.Is(err, task.ErrRequestGUIDUnavailable):
		httpx.WriteError(c, http.StatusConflict, codeRequestGUIDUnavailable, messageRequestGUIDTaken)
	case errors.Is(err, task.ErrStorageUnavailable):
		slog.WarnContext(c.Request.Context(), "storage unavailable", "error", err, "path", c.FullPath())
		httpx.WriteServiceUnavailable(c)
	default:
		slog.ErrorContext(c.Request.Context(), "task request failed", "error", err, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}
}
