package middleware

import (
	"time"

	"github.com/sony/gobreaker"
	_ "github.com/sony/gobreaker"
)

type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

type CircuitBreakerConfig struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
	OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
}

func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	settings := gobreaker.Settings{
		Name:          config.Name,
		MaxRequests:   config.MaxRequests,
		Interval:      config.Interval,
		Timeout:       config.Timeout,
		ReadyToTrip:   config.ReadyToTrip,
		OnStateChange: config.OnStateChange,
	}

	if settings.ReadyToTrip == nil {
		settings.ReadyToTrip = func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		}
	}

	return &CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	_, err := cb.cb.Execute(func() (interface{}, error) {
		return nil, fn()
	})
	return err
}

func (cb *CircuitBreaker) State() gobreaker.State {
	return cb.cb.State()
}
