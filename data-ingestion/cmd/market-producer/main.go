package marketproducer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FinStreamAl/data-ingestion/internal/producer"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type MarketProducer struct {
	baseProducer  *producer.BaseProducer
	wsClient      *websocket.Conn
	config        *MarketConfig
	logger        *zap.SugaredLogger
	rateLimiter   *rate.Limiter
	reconnectChan chan struct{}
}

type MarketConfig struct {
	WebSocketURL   string
	APIKey         string
	Symbols        []string
	ReconnectDelay time.Duration
}

type MarketTick struct {
	Symbol     string    `json:"symbol"`
	Price      float64   `json:"price"`
	Volume     float64   `json:"volume"`
	Timestamp  time.Time `json:"timestamp"`
	BidPrice   float64   `json:"bid_price"`
	AskPrice   float64   `json:"ask_price"`
	BidSize    int64     `json:"bid_size"`
	AskSize    int64     `json:"ask_size"`
	Exchange   string    `json:"exchange"`
	Conditions []string  `json:"conditions,omitempty"`
}

type WebSocketMessage struct {
	EventType string    `json:"ev"`
	Symbol    string    `json:"sym"`
	Price     float64   `json:"p"`
	Volume    float64   `json:"v"`
	Timestamp time.Time `json:"t"`
	Bid       float64   `json:"bp,omitempty"`
	Ask       float64   `json:"ap,omitempty"`
	BidSize   int64     `json:"bs,omitempty"`
	AskSize   int64     `json:"as,omitempty"`
	Exchange  string    `json:"x,omitempty"`
}

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	config := loadConfig()

	producerConfig := &producer.Config{
		ProducerName:    "market-producer",
		Topic:           "market-producer",
		KafkaBrokers:    []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistry:  "http://schema-registry/8081",
		BatchSize:       100,
		FlushInterval:   10 * time.Millisecond,
		RateLimitPerSec: 10000,
		RetryAttempts:   5,
		RetryBackoff:    100 * time.Millisecond,
	}

	baseProducer, err := producer.NewBaseProducer(producerConfig, &zapLoggerAdapter{logger})
	if err != nil {
		logger.Fatalf("Error creating base producer: %v", err)
	}

	marketProducer := &MarketProducer{
		baseProducer: baseProducer,
		config:       config,
		logger:       logger,
		rateLimiter: rate.NewLimiter(rate.Limit(producerConfig.RateLimitPerSec),
			producerConfig.RateLimitPerSec),
		reconnectChan: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := marketProducer.Start(ctx); err != nil {
			logger.Errorf("Error starting market-producer: %v", err)
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down market-producer...")
	cancel()

	if err := marketProducer.Stop(); err != nil {
		logger.Errorf("Error stopping market-producer: %v", err)
		os.Exit(1)
	}

	logger.Info("Market producer stopped successfully")
}

func (mp *MarketProducer) Start(ctx context.Context) error {
	mp.logger.Info("Starting market data producer")

	for {
		select {
		case <-ctx.Done():
			mp.logger.Info("Shutting down market-producer...")
			return nil
		default:
			if err := mp.connectWebSocket(); err != nil {
				mp.logger.Errorf("Error connecting to market-producer: %v", err)
				time.Sleep(mp.config.ReconnectDelay)
				continue
			}

			if err := mp.subscribe(); err != nil {
				mp.logger.Errorf("Error subscribing to market-producer: %v", err)
				mp.wsClient.Close()
				time.Sleep(mp.config.ReconnectDelay)
				continue
			}

			if err := mp.consumeMessage(ctx); err != nil {
				mp.logger.Errorf("Error consuming market-producer: %v", err)
				mp.wsClient.Close()
				time.Sleep(mp.config.ReconnectDelay)
				continue
			}
		}
	}
}

func (mp *MarketProducer) connectWebSocket() error {
	mp.logger.Info("Connecting to market-producer...", "url", mp.config.WebSocketURL)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(mp.config.WebSocketURL, nil)

	if err != nil {
		mp.logger.Errorf("Error connecting to market-producer: %v", err)
	}

	mp.wsClient = conn
	mp.logger.Info("Connected to market-producer")
	return nil
}

func (mp *MarketProducer) subscribe() error {
	authMsg := map[string]interface{}{
		"action": "auth",
		"params": mp.config.APIKey,
	}

	if err := mp.wsClient.WriteJSON(authMsg); err != nil {
		mp.logger.Errorf("Error subscribing to market-producer: %v", err)
		return err
	}

	subscribeMsg := map[string]interface{}{
		"action": "subscribe",
		"params": mp.config.Symbols,
	}

	if err := mp.wsClient.WriteJSON(subscribeMsg); err != nil {
		mp.logger.Errorf("Error subscribing to market-producer: %v", err)
		return err
	}

	mp.logger.Info("Subscribed to market-producer")
	return nil
}

func (mp *MarketProducer) consumeMessage(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			mp.wsClient.SetReadDeadline(time.Now().Add(60 * time.Second))

			var wsMsg []WebSocketMessage

			err := mp.wsClient.ReadJSON(&wsMsg)

			if err != nil {

				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					return fmt.Errorf("Webscoket closed unexpectedly: %w", err)
				}

				return fmt.Errorf("Error reading message: %v", err)
			}

			for _, msg := range wsMsg {
				if err := mp.processMessage(ctx, msg); err != nil {
					mp.logger.Errorf("Error processing message: %v", err)
					mp.baseProducer.IncrementMessageFailed("process-message-error")
				}
			}
		}
	}
}

func (mp *MarketProducer) processMessage(ctx context.Context, wsMsg WebSocketMessage) error {
	if err := mp.rateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("Error waiting for rate limiter: %v", err)
	}

	tick := MarketTick{
		Symbol:    wsMsg.Symbol,
		Price:     wsMsg.Price,
		Volume:    wsMsg.Volume,
		Timestamp: time.UnixMilli(wsMsg.Timestamp.UnixMilli()),
		BidPrice:  wsMsg.Bid,
		AskPrice:  wsMsg.Ask,
		AskSize:   wsMsg.AskSize,
		BidSize:   wsMsg.BidSize,
		Exchange:  wsMsg.Exchange,
	}

	if err := mp.validateTick(&tick); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	value, err := json.Marshal(tick)

	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := []byte(tick.Symbol)

	startTme := time.Now()

	if err := mp.baseProducer.PublishMessage(ctx, key, value); err != nil {
		mp.baseProducer.IncrementMessageFailed("kafka_error")
		return fmt.Errorf("Error publishing message: %w", err)
	}

	mp.baseProducer.IncrementMessageSent(int64(len(value)))
	mp.baseProducer.RecordAPICall("kafka_publish", time.Since(startTme))
	return nil
}

func (mp *MarketProducer) validateTick(tick *MarketTick) error {
	if tick.Symbol == "" {
		return fmt.Errorf("empty symbol")
	}

	if tick.Price <= 0 {
		return fmt.Errorf("invalid price: %f", tick.Price)
	}

	if tick.Volume < 0 {
		return fmt.Errorf("negative volumne: %d", tick.Volume)
	}

	if tick.Timestamp.IsZero() {
		return fmt.Errorf("empty timestamp")
	}
	return nil
}

func (mp *MarketProducer) Stop() error {
	mp.logger.Info("Stopping market producer...")

	if mp.wsClient != nil {
		mp.wsClient.Close()
	}
	return mp.baseProducer.Stop()
}

type zapLoggerAdapter struct {
	logger *zap.SugaredLogger
}

func (z *zapLoggerAdapter) Info(msg string, fields ...interface{}) {
	z.logger.Infow(msg, fields...)
}

func (z *zapLoggerAdapter) Error(msg string, err error, fields ...interface{}) {
	z.logger.Errorw(msg, append(fields, "error", err)...)
}

func (z *zapLoggerAdapter) Debug(msg string, fields ...interface{}) {
	z.logger.Debugw(msg, fields...)
}

func (z *zapLoggerAdapter) Warn(msg string, fields ...interface{}) {
	z.logger.Warnw(msg, append(fields)...)
}

func loadConfig() *MarketConfig {
	return &MarketConfig{
		WebSocketURL: getEnv("MARKET_WS_URL", "wss://socket.polygon.io/stocks"),
		APIKey:       getEnv("MARKET_API_KEY", ""),
		//todo change the string value to the dynamics type from the users or something else
		Symbols:        []string{"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"},
		ReconnectDelay: 5 * time.Second,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
