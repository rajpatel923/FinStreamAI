package producer

import (
	"context"
	"time"
)

type Producer interface {
	Start(ctx context.Context) error
	Stop() error
	HealthCheck() error
	GetMatrics() *Metrics
	Name() string
}

type BaseProducer struct {
	name          string
	kafkaProducer *KafkaProducer
	metrics       *Metrics
	logger        Logger
	config        *Config
	stopChan      chan struct{}
	doneChan      chan struct{}
}

type Config struct {
	ProducerName    string
	KafkaBrokers    []string
	Topic           string
	SchemaRegistry  string
	BatchSize       int
	FlushInterval   time.Duration
	RateLimitPerSec int
	RetryAttempts   int
	RetryBackoff    time.Duration
}

type Metrics struct {
	MessagesSent       int64
	MessagesFailed     int64
	BytesSent          int64
	LastMessageTime    time.Time
	ErrorCount         map[string]int64
	APICallDuration    map[string]time.Duration
	CircuitBreakerOpen bool
}

type Logger interface {
	Info(msg string, fields ...interface{})
	Debug(msg string, fields ...interface{})
	Error(msg string, err error, fields ...interface{})
	Warn(msg string, fields ...interface{})
}

func NewBaseProducer(config *Config, logger Logger) (*BaseProducer, error) {

	kafkaProducer, err := NewkafkaProducer(config, logger)
	if err != nil {
		return nil, err
	}

	return &BaseProducer{
		name:          config.ProducerName,
		kafkaProducer: kafkaProducer,
		metrics:       &Metrics{ErrorCount: make(map[string]int64), APICallDuration: make(map[string]time.Duration)},
		logger:        logger,
		config:        config,
		stopChan:      make(chan struct{}),
		doneChan:      make(chan struct{}),
	}, nil
}

func (bp *BaseProducer) PublishMessage(ctx context.Context, key []byte, value []byte) error {
	return bp.kafkaProducer.Publish(ctx, bp.config.Topic, key, value)
}

func (bp *BaseProducer) Stop() error {
	close(bp.stopChan)
	select {
	case <-bp.doneChan:
		bp.logger.Info("Producer stoppped gracefully", "producer", bp.name)
	case <-time.After(30 * time.Second):
		bp.logger.Warn("Producer timed out after 30 seconds", "producer", bp.name)
	}
	return bp.kafkaProducer.Close()
}

func (bp *BaseProducer) Name() string {
	return bp.name
}

func (bp *BaseProducer) HealthCheck() error {
	if err := bp.kafkaProducer.Ping(); err != nil {
		return err
	}

	if time.Since(bp.metrics.LastMessageTime) > 5*time.Minute && bp.metrics.MessagesSent > 0 {
		bp.logger.Warn("No messages sent recently", "producer", bp.name, "lastNessage", bp.metrics.LastMessageTime)
	}
	return nil
}

func (bp *BaseProducer) IncrementMessageSent(bytes int64) {
	bp.metrics.MessagesSent++
	bp.metrics.BytesSent += bytes
	bp.metrics.LastMessageTime = time.Now()
}

func (bp *BaseProducer) IncrementMessageFailed(errorType string) {
	bp.metrics.MessagesFailed++
	bp.metrics.ErrorCount[errorType]++
}

func (bp *BaseProducer) RecordAPICall(apiName string, duration time.Duration) {
	bp.metrics.APICallDuration[apiName] = duration
}

func (bp *BaseProducer) ShouldStop() bool {
	select {
	case <-bp.stopChan:
		return true
	default:
		return false
	}
}

func (bp *BaseProducer) Done() {
	close(bp.doneChan)
}
