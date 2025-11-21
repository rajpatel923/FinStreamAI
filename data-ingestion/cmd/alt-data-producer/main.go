package altdataproducer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FinStreamAl/data-ingestion/internal/middleware"
	"github.com/FinStreamAl/data-ingestion/internal/producer"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type AltDataProducer struct {
	baseProducer     *producer.BaseProducer
	httpClient       *http.Client
	config           *AltDataConfig
	logger           *zap.SugaredLogger
	satelliteLimiter *rate.Limiter
	scrapingLimiter  *rate.Limiter
	satelliteBreaker *middleware.CircuitBreaker
	scrapingBreaker  *middleware.CircuitBreaker
}

type AltDataConfig struct {
	SatelliteAPIURL   string
	SatelliteAPIKey   string
	SatelliteInterval time.Duration
	ScrapingTargets   []ScrapingTarget
	ScrapingInterval  time.Duration
}

type ScrapingTarget struct {
	URL      string
	Name     string
	Selector string
}

type AltDataPoint struct {
	ID          string                 `json:"id"`
	Source      string                 `json:"source"`
	DataType    string                 `json:"data_type"`
	Location    string                 `json:"location,omitempty"`
	Value       interface{}            `json:"value"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CollectedAt time.Time              `json:"collected_at"`
	Timestamp   time.Time              `json:"timestamp"`
}

type SatelliteImageryResponse struct {
	Data []struct {
		ID         string                 `json:"id"`
		Location   string                 `json:"location"`
		ImageURL   string                 `json:"image_url"`
		Metadata   map[string]interface{} `json:"metadata"`
		CapturedAt time.Time              `json:"captured_at"`
	} `json:"data"`
}

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	config := loadConfig()

	producerConfig := &producer.Config{
		ProducerName:    "alt-data-producer",
		Topic:           "alt-data-producer",
		KafkaBrokers:    []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistry:  "http://schema-registry:8081",
		BatchSize:       10,
		FlushInterval:   10 * time.Second,
		RateLimitPerSec: 10,
		RetryAttempts:   5,
		RetryBackoff:    100 * time.Millisecond,
	}

	baseProducer, err := producer.NewBaseProducer(producerConfig, &zapLoggerAdapter{logger})
	if err != nil {
		logger.Fatalf("Error creating base producer: %v", err)
	}

	altDataProducer := &AltDataProducer{
		baseProducer:     baseProducer,
		config:           config,
		logger:           logger,
		httpClient:       &http.Client{Timeout: 60 * time.Second},
		satelliteLimiter: rate.NewLimiter(rate.Every(10*time.Second), 1), // Conservative rate
		scrapingLimiter:  rate.NewLimiter(rate.Every(5*time.Second), 1),
		satelliteBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "satellite",
			MaxRequests: 5,
			Interval:    60 * time.Second,
			Timeout:     120 * time.Second,
		}),
		scrapingBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "scraping",
			MaxRequests: 5,
			Interval:    60 * time.Second,
			Timeout:     120 * time.Second,
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := altDataProducer.Start(ctx); err != nil {
			logger.Errorf("Error starting alt-data-producer: %v", err)
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down alt-data-producer...")
	cancel()

	if err := altDataProducer.Stop(); err != nil {
		logger.Errorf("Error stopping alt-data-producer: %v", err)
		os.Exit(1)
	}

	logger.Info("Alt-data producer stopped successfully")
}

func (adp *AltDataProducer) Start(ctx context.Context) error {
	adp.logger.Info("Starting alt-data producer")

	satelliteTicker := time.NewTicker(adp.config.SatelliteInterval)
	scrapingTicker := time.NewTicker(adp.config.ScrapingInterval)

	defer satelliteTicker.Stop()
	defer scrapingTicker.Stop()

	// Initial fetch
	adp.fetchSatelliteData(ctx)
	adp.fetchScrapedData(ctx)

	for {
		select {
		case <-ctx.Done():
			adp.logger.Info("Shutting down alt-data producer...")
			return nil
		case <-satelliteTicker.C:
			adp.fetchSatelliteData(ctx)
		case <-scrapingTicker.C:
			adp.fetchScrapedData(ctx)
		}
	}
}

func (adp *AltDataProducer) fetchSatelliteData(ctx context.Context) {
	if err := adp.satelliteLimiter.Wait(ctx); err != nil {
		adp.logger.Errorf("Rate limiter error for satellite data: %v", err)
		return
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
	}

	err := middleware.Retry(ctx, retryConfig, func() error {
		return adp.satelliteBreaker.Execute(func() error {
			return adp.fetchSatelliteDataInternal(ctx)
		})
	})

	if err != nil {
		adp.logger.Errorf("Failed to fetch satellite data after retries: %v", err)
		adp.baseProducer.IncrementMessageFailed("satellite_fetch_error")
	}
}

func (adp *AltDataProducer) fetchSatelliteDataInternal(ctx context.Context) error {
	url := fmt.Sprintf("%s?apikey=%s", adp.config.SatelliteAPIURL, adp.config.SatelliteAPIKey)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	startTime := time.Now()
	resp, err := adp.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch satellite data: %w", err)
	}
	defer resp.Body.Close()

	adp.baseProducer.RecordAPICall("satellite_fetch", time.Since(startTime))

	if resp.StatusCode == 429 {
		return fmt.Errorf("rate limit exceeded")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var satResp SatelliteImageryResponse
	if err := json.Unmarshal(body, &satResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	adp.logger.Infof("Fetched %d satellite imagery records", len(satResp.Data))

	for _, item := range satResp.Data {
		dataPoint := AltDataPoint{
			ID:          adp.generateID(item.ID),
			Source:      "satellite",
			DataType:    "imagery",
			Location:    item.Location,
			Value:       item.ImageURL,
			Metadata:    item.Metadata,
			CollectedAt: item.CapturedAt,
			Timestamp:   time.Now(),
		}

		if err := adp.publishDataPoint(ctx, dataPoint); err != nil {
			adp.logger.Errorf("Failed to publish satellite data point: %v", err)
		}
	}

	return nil
}

func (adp *AltDataProducer) fetchScrapedData(ctx context.Context) {
	for _, target := range adp.config.ScrapingTargets {
		if err := adp.scrapeTarget(ctx, target); err != nil {
			adp.logger.Errorf("Failed to scrape %s: %v", target.Name, err)
		}
	}
}

func (adp *AltDataProducer) scrapeTarget(ctx context.Context, target ScrapingTarget) error {
	if err := adp.scrapingLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter error: %w", err)
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 3 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
	}

	return middleware.Retry(ctx, retryConfig, func() error {
		return adp.scrapingBreaker.Execute(func() error {
			return adp.scrapeTargetInternal(ctx, target)
		})
	})
}

func (adp *AltDataProducer) scrapeTargetInternal(ctx context.Context, target ScrapingTarget) error {
	req, err := http.NewRequestWithContext(ctx, "GET", target.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Anti-bot headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")

	startTime := time.Now()
	resp, err := adp.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch page: %w", err)
	}
	defer resp.Body.Close()

	adp.baseProducer.RecordAPICall("scraping_fetch", time.Since(startTime))

	if resp.StatusCode == 429 {
		return fmt.Errorf("rate limit exceeded")
	}

	if resp.StatusCode == 403 {
		return fmt.Errorf("access forbidden (anti-bot protection)")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Simple text extraction (in production, use a proper HTML parser like goquery)
	content := string(body)
	extractedValue := adp.extractWithSelector(content, target.Selector)

	if extractedValue == "" {
		adp.logger.Warnf("No data extracted from %s using selector %s", target.Name, target.Selector)
		return nil
	}

	dataPoint := AltDataPoint{
		ID:       adp.generateID(target.URL + time.Now().Format(time.RFC3339)),
		Source:   "web-scraping",
		DataType: target.Name,
		Value:    extractedValue,
		Metadata: map[string]interface{}{
			"url":      target.URL,
			"selector": target.Selector,
		},
		CollectedAt: time.Now(),
		Timestamp:   time.Now(),
	}

	if err := adp.publishDataPoint(ctx, dataPoint); err != nil {
		return fmt.Errorf("failed to publish data point: %w", err)
	}

	adp.logger.Infof("Successfully scraped data from %s", target.Name)
	return nil
}

func (adp *AltDataProducer) extractWithSelector(content, selector string) string {
	// Simple extraction logic - in production use a proper HTML parser
	// This is a placeholder that looks for text between tags
	if selector == "" {
		return content[:min(500, len(content))]
	}

	// Basic tag extraction (example: extract text within <title> tags)
	startTag := "<" + selector + ">"
	endTag := "</" + selector + ">"

	startIdx := strings.Index(content, startTag)
	if startIdx == -1 {
		return ""
	}

	startIdx += len(startTag)
	endIdx := strings.Index(content[startIdx:], endTag)
	if endIdx == -1 {
		return ""
	}

	extracted := content[startIdx : startIdx+endIdx]
	return strings.TrimSpace(extracted)
}

func (adp *AltDataProducer) publishDataPoint(ctx context.Context, dataPoint AltDataPoint) error {
	if err := adp.validateDataPoint(&dataPoint); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	value, err := json.Marshal(dataPoint)
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := []byte(dataPoint.ID)

	startTime := time.Now()
	if err := adp.baseProducer.PublishMessage(ctx, key, value); err != nil {
		adp.baseProducer.IncrementMessageFailed("kafka_error")
		return fmt.Errorf("error publishing message: %w", err)
	}

	adp.baseProducer.IncrementMessageSent(int64(len(value)))
	adp.baseProducer.RecordAPICall("kafka_publish", time.Since(startTime))

	return nil
}

func (adp *AltDataProducer) validateDataPoint(dataPoint *AltDataPoint) error {
	if dataPoint.ID == "" {
		return fmt.Errorf("empty data point ID")
	}

	if dataPoint.Source == "" {
		return fmt.Errorf("empty source")
	}

	if dataPoint.DataType == "" {
		return fmt.Errorf("empty data type")
	}

	if dataPoint.Value == nil {
		return fmt.Errorf("nil value")
	}

	if dataPoint.CollectedAt.IsZero() {
		return fmt.Errorf("empty collected_at timestamp")
	}

	return nil
}

func (adp *AltDataProducer) generateID(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash[:8])
}

func (adp *AltDataProducer) Stop() error {
	adp.logger.Info("Stopping alt-data producer...")
	return adp.baseProducer.Stop()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

func loadConfig() *AltDataConfig {
	return &AltDataConfig{
		SatelliteAPIURL:   getEnv("SATELLITE_API_URL", "https://api.satellite-provider.com/v1/imagery"),
		SatelliteAPIKey:   getEnv("SATELLITE_API_KEY", ""),
		SatelliteInterval: 3600 * time.Second, // 1 hour
		ScrapingTargets: []ScrapingTarget{
			{
				URL:      getEnv("SCRAPING_TARGET_1_URL", "https://example.com/data"),
				Name:     getEnv("SCRAPING_TARGET_1_NAME", "example-data"),
				Selector: getEnv("SCRAPING_TARGET_1_SELECTOR", "title"),
			},
		},
		ScrapingInterval: 600 * time.Second, // 10 minutes
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
