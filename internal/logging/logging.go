package logging

import (
	"context"
	"github.com/draculaas/shrek/internal/core"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LogKey = string

type loggerKeyType int

const loggerKey loggerKeyType = iota

const (
	messageKey = "message"
)

var logger *zap.Logger = zap.NewNop()

// NewContext ... A helper for middleware to create requestId or other context fields
// and return a context which logger can understand.
func NewContext(ctx context.Context, fields ...zap.Field) context.Context {
	return context.WithValue(ctx, loggerKey, WithContext(ctx).With(fields...))
}

// WithContext ... Pass in a context containing values to add to each log message
func WithContext(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return logger
	}

	if ctxLogger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return logger
}

// NoContext ... A log helper to log when there's no context. Rare case usage
func NoContext() *zap.Logger {
	return logger
}

// New ... A helper to create a logger based on environment
func New(env core.Env) {
	switch env {
	case core.Local:
		logger = NewLocal()
	default:
		panic("Invalid environment")
	}
}

func NewLocal() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.EncoderConfig.MessageKey = messageKey

	logger, err := cfg.Build(zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		panic(err)
	}

	return logger
}
