package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/d9293/halyk-genai-integrator/internal/config"
	httphandler "github.com/d9293/halyk-genai-integrator/internal/handler/http"
	taskhandler "github.com/d9293/halyk-genai-integrator/internal/handler/http/task"
	"github.com/d9293/halyk-genai-integrator/internal/repository/postgres"
	taskrepo "github.com/d9293/halyk-genai-integrator/internal/repository/postgres/task"
	taskusecase "github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

// Run wires the app and serves HTTP until ctx is cancelled.
func Run(ctx context.Context, cfg config.Config) error {
	gin.SetMode(gin.ReleaseMode)

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	tasks, err := taskusecase.NewUseCase(taskrepo.NewRepo(pool, cfg.Postgres.QueryTimeout), taskusecase.Config{
		EnabledOperations: cfg.EnabledOperations,
		AttstoreHosts:     cfg.AttstoreHosts,
	})
	if err != nil {
		return fmt.Errorf("create task usecase: %w", err)
	}

	router := httphandler.NewRouter(cfg.CallerID, cfg.SwaggerEnabled, pool.Ping, taskhandler.NewHandler(tasks))
	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           router.Engine(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("http server started",
			"port", cfg.HTTPPort,
			"postgres", cfg.Postgres.Host+":"+cfg.Postgres.Port+"/"+cfg.Postgres.Database,
			"swagger", cfg.SwaggerEnabled,
			"operations", cfg.EnabledOperations,
		)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	return nil
}
