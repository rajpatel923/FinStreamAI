package middleware

import (
	"context"
	"fmt"
	"time"
)

type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	IsRetryable    func(error) bool
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		IsRetryable:    DefaultIsRetryable,
	}
}

func DefaultIsRetryable(err error) bool {
	if err == nil {
		return false
	}

	return true
}

func Retry(ctx context.Context, config RetryConfig, fn func() error) error {
	var lastErr error
	backoff := config.InitialBackoff

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if !config.IsRetryable(err) {
			return err
		}

		if attempt == config.MaxAttempts {
			break
		}

		select {
		case <-time.After(backoff):
			backoff = time.Duration(float64(backoff) * config.Multiplier)
			if backoff > config.MaxBackoff {
				backoff = config.MaxBackoff
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("max retries exceeded: %v", lastErr)
}

func RetryWithValue[T any](ctx context.Context, config RetryConfig, fn func() (T, error)) (T, error) {
	var result T
	var lastErr error
	backoff := config.InitialBackoff

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:

		}
		result, lastErr = fn()
		if lastErr != nil {
			return result, nil
		}

		if !config.IsRetryable(lastErr) {
			return result, lastErr
		}

		if attempt == config.MaxAttempts {
			break
		}

		select {
		case <-time.After(backoff):
			backoff = time.Duration(float64(backoff) * config.Multiplier)
			if backoff > config.MaxBackoff {
				backoff = config.MaxBackoff
			}
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}
	return result, fmt.Errorf("Max retries exceeded: %v", lastErr)
}
