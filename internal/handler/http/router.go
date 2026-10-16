package http

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/document"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/poll"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/submission"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/task"
)

const readinessTimeout = 2 * time.Second

type Router struct {
	callerID       string
	swaggerEnabled bool
	// checkReady returns an error when a dependency the API needs (the database) is unreachable.
	checkReady        func(ctx context.Context) error
	taskHandler       *task.Handler
	documentHandler   *document.Handler
	submissionHandler *submission.Handler
	pollHandler       *poll.Handler
}

func NewRouter(
	callerID string,
	swaggerEnabled bool,
	checkReady func(ctx context.Context) error,
	taskHandler *task.Handler,
	documentHandler *document.Handler,
	submissionHandler *submission.Handler,
	pollHandler *poll.Handler,
) *Router {
	return &Router{
		callerID:          callerID,
		swaggerEnabled:    swaggerEnabled,
		checkReady:        checkReady,
		taskHandler:       taskHandler,
		documentHandler:   documentHandler,
		submissionHandler: submissionHandler,
		pollHandler:       pollHandler,
	}
}

func (router *Router) Engine() *gin.Engine {
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(gin.CustomRecovery(func(c *gin.Context, recovered any) {
		slog.ErrorContext(c.Request.Context(), "panic in handler", "panic", recovered, "path", c.FullPath())
		httpx.WriteError(c, http.StatusInternalServerError, httpx.CodeInternalError, "Внутренняя ошибка")
	}))
	r.NoRoute(func(c *gin.Context) {
		httpx.WriteError(c, http.StatusNotFound, httpx.CodeNotFound, "Маршрут не найден")
	})
	r.NoMethod(func(c *gin.Context) {
		httpx.WriteError(c, http.StatusMethodNotAllowed, httpx.CodeMethodNotAllowed, "Метод не поддерживается")
	})

	// Liveness never checks dependencies: a database outage must not restart the pod.
	r.GET("/health/liveness", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/health/readiness", router.readiness)
	if router.swaggerEnabled {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	// BPM-facing API.
	api := r.Group("/api/v1", httpx.FixedCaller(router.callerID))
	router.taskHandler.Routes(api)

	// API for our own workers.
	internal := r.Group("/internal/v1")
	router.documentHandler.Routes(internal)
	router.submissionHandler.Routes(internal)
	router.pollHandler.Routes(internal)

	return r
}

func (router *Router) readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
	defer cancel()

	if err := router.checkReady(ctx); err != nil {
		slog.WarnContext(ctx, "not ready", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
