package http

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/task"
)

type Router struct {
	callerID       string
	swaggerEnabled bool
	taskHandler    *task.Handler
}

func NewRouter(callerID string, swaggerEnabled bool, taskHandler *task.Handler) *Router {
	return &Router{
		callerID:       callerID,
		swaggerEnabled: swaggerEnabled,
		taskHandler:    taskHandler,
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

	r.GET("/health/liveness", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	if router.swaggerEnabled {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	api := r.Group("/api/v1", httpx.FixedCaller(router.callerID))
	router.taskHandler.Routes(api)

	return r
}
