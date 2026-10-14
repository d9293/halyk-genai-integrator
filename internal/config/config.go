// Package config is the only place that reads configuration. The app reads
// environment variables only, so swapping the .env file for the corporate
// env/Consul library later changes this package and nothing else.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

type Config struct {
	HTTPPort        string        `env:"HTTP_PORT,required"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT,required"`
	SwaggerEnabled  bool          `env:"SWAGGER_ENABLED,required"`
	// CallerID identifies BPM while auth is deferred (decision 2026-10-13).
	CallerID          string             `env:"CALLER_ID,required"`
	EnabledOperations []domain.Operation `env:"ENABLED_OPERATIONS,required"`
	AttstoreHosts     []string           `env:"ATTSTORE_ALLOWED_HOSTS,required"`
}

// Load reads the env file into the process environment, then parses the
// environment into Config. Variables already set in the environment win over
// the file.
func Load(path string) (Config, error) {
	if err := godotenv.Load(path); err != nil {
		return Config{}, fmt.Errorf("load config file %s: %w", path, err)
	}

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
