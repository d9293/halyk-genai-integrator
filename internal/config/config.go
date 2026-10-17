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
	// DocumentClaimLease is how long a worker owns a claimed document without renewing.
	DocumentClaimLease time.Duration `env:"DOCUMENT_CLAIM_LEASE,required"`
	// A failed copy is retried after DocumentRetryBaseDelay, doubling up to
	// DocumentRetryMaxDelay; after DocumentMaxCopyAttempts failures the
	// document and its task need attention.
	DocumentMaxCopyAttempts int           `env:"DOCUMENT_MAX_COPY_ATTEMPTS,required"`
	DocumentRetryBaseDelay  time.Duration `env:"DOCUMENT_RETRY_BASE_DELAY,required"`
	DocumentRetryMaxDelay   time.Duration `env:"DOCUMENT_RETRY_MAX_DELAY,required"`
	// SubmissionClaimLease is how long a worker owns a POST to AI in progress without renewing.
	SubmissionClaimLease time.Duration `env:"SUBMISSION_CLAIM_LEASE,required"`
	// AIMaxInFlight is how many tasks may be at AI at once (sent, queued or running there).
	AIMaxInFlight int `env:"AI_MAX_IN_FLIGHT,required"`
	// SubmissionPollDelay is when the first status check happens after a task was sent.
	SubmissionPollDelay time.Duration `env:"SUBMISSION_POLL_DELAY,required"`
	// PollClaimLease is how long worker.Response owns one status check (a single GET to AI).
	PollClaimLease time.Duration `env:"POLL_CLAIM_LEASE,required"`
	// Next status check after QUEUED/PROCESSING: PollInterval ± PollIntervalJitter.
	PollInterval       time.Duration `env:"POLL_INTERVAL,required"`
	PollIntervalJitter time.Duration `env:"POLL_INTERVAL_JITTER,required"`
	// Next status check after AI said it does not know the task.
	PollNotFoundDelay time.Duration `env:"POLL_NOT_FOUND_DELAY,required"`
	// Next status check after a timeout or other technical error.
	PollErrorDelay time.Duration `env:"POLL_ERROR_DELAY,required"`
	// A task is lost after LostTaskNotFoundThreshold NOT_FOUND answers in a
	// row spread over at least LostTaskMinWindow; it is then sent again, at
	// most MaxResubmissions times.
	LostTaskNotFoundThreshold int           `env:"LOST_TASK_NOT_FOUND_THRESHOLD,required"`
	LostTaskMinWindow         time.Duration `env:"LOST_TASK_MIN_WINDOW,required"`
	MaxResubmissions          int           `env:"MAX_RESUBMISSIONS,required"`
	// ResultMaxBytes caps the stored AI result; a bigger one needs attention.
	ResultMaxBytes int `env:"RESULT_MAX_BYTES,required"`
	// KafkaResultTopic is where final task results are published for BPM.
	KafkaResultTopic string `env:"KAFKA_RESULT_TOPIC,required"`
	// ResultClaimLease is how long worker.Response owns one result while publishing it.
	ResultClaimLease time.Duration `env:"RESULT_CLAIM_LEASE,required"`
	// A result Kafka did not accept is tried again after ResultRetryBaseDelay,
	// doubling up to ResultRetryMaxDelay. It is never dropped.
	ResultRetryBaseDelay time.Duration `env:"RESULT_RETRY_BASE_DELAY,required"`
	ResultRetryMaxDelay  time.Duration `env:"RESULT_RETRY_MAX_DELAY,required"`
	Postgres             PostgresConfig
}

type PostgresConfig struct {
	Host     string `env:"POSTGRES_HOST,required"`
	Port     string `env:"POSTGRES_PORT,required"`
	Database string `env:"POSTGRES_DB,required"`
	User     string `env:"POSTGRES_USER,required"`
	Password string `env:"POSTGRES_PASSWORD,required"`
	SSLMode  string `env:"POSTGRES_SSL_MODE,required"`
	MaxConns int32  `env:"POSTGRES_MAX_CONNS,required"`
	// QueryTimeout bounds every database call, so a stuck database turns into a 503.
	QueryTimeout time.Duration `env:"POSTGRES_QUERY_TIMEOUT,required"`
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
