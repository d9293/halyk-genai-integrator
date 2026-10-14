// Integrator.Api: BPM-facing API of the BPM ↔ AI integration.
//
//	@title			Integrator.Api
//	@version		1.0
//	@description	Регистрация и чтение задач BPM.
//	@BasePath		/
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/d9293/halyk-genai-integrator/internal/app"
	"github.com/d9293/halyk-genai-integrator/internal/config"

	_ "github.com/d9293/halyk-genai-integrator/docs/swagger"
)

func main() {
	configFile := flag.String("config_file", "", "path to the env config file, e.g. dev.env (required)")
	flag.Parse()

	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "--config_file is required, e.g. go run . --config_file dev.env")
		os.Exit(2)
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg); err != nil {
		slog.Error("app stopped", "error", err)
		os.Exit(1)
	}
}
