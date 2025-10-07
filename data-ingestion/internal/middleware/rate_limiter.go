package middleware

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	limiter *rate.Limiter
	mu      sync.RWMutex
}

func NewRateLimiter(requestPerSecond int) *RateLimiter {
	return &RateLimiter{
		limiter: rate.NewLimiter(rate.Limit(requestPerSecond), requestPerSecond),
	}
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.limiter.Wait(ctx)
}

func (rl *RateLimiter) Allow() bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.limiter.Allow()
}

func (rl *RateLimiter) UpdateLimit(requestPerSecond int) {
	rl.mu.Lock()

	defer rl.mu.Unlock()

	rl.limiter.SetLimit(rate.Limit(requestPerSecond))
	rl.limiter.SetBurst(requestPerSecond)
}

type MultiRateLimiter struct {
	limiters map[string]*RateLimiter
	mu       sync.RWMutex
}

func NewMultiRateLimiter() *MultiRateLimiter {
	return &MultiRateLimiter{
		limiters: make(map[string]*RateLimiter),
	}
}

func (mrl *MultiRateLimiter) AddLimiter(resource string, requestPerSecond int) {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()
	mrl.limiters[resource] = NewRateLimiter(requestPerSecond)
}

func (mrl *MultiRateLimiter) Wait(ctx context.Context, resource string) error {
	mrl.mu.RLock()
	limiter, exists := mrl.limiters[resource]
	mrl.mu.RUnlock()
	if !exists {
		return errors.New("rate limiter not found for resource: " + resource)
	}

	return limiter.Wait(ctx)
}

func (mrl *MultiRateLimiter) Allow(resource string) bool {
	mrl.mu.RLock()
	limiter, exists := mrl.limiters[resource]
	mrl.mu.RUnlock()
	if !exists {
		return false
	}
	return limiter.Allow()
}
