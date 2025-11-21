package newsproducer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FinStreamAl/data-ingestion/internal/middleware"
	"github.com/FinStreamAl/data-ingestion/internal/producer"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type NewsProducer struct {
	baseProducer        *producer.BaseProducer
	httpClient          *http.Client
	config              *NewsConfig
	logger              *zap.SugaredLogger
	newsAPILimiter      *rate.Limiter
	alphaVantageLimiter *rate.Limiter
	newsAPIBreaker      *middleware.CircuitBreaker
	alphaVantageBreaker *middleware.CircuitBreaker
	dedupeCache         map[string]time.Time
}

type NewsConfig struct {
	NewsAPIKey           string
	NewsAPIURL           string
	AlphaVantageKey      string
	AlphaVantageURL      string
	NewsAPIInterval      time.Duration
	AlphaVantageInterval time.Duration
	DedupeWindow         time.Duration
	Query                string
}

type NewsArticle struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	URL         string    `json:"url"`
	PublishedAt time.Time `json:"published_at"`
	Author      string    `json:"author,omitempty"`
	Content     string    `json:"content,omitempty"`
	Sentiment   string    `json:"sentiment,omitempty"`
	Symbols     []string  `json:"symbols,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

type NewsAPIResponse struct {
	Status   string `json:"status"`
	Articles []struct {
		Source struct {
			Name string `json:"name"`
		} `json:"source"`
		Author      string    `json:"author"`
		Title       string    `json:"title"`
		Description string    `json:"description"`
		URL         string    `json:"url"`
		PublishedAt time.Time `json:"publishedAt"`
		Content     string    `json:"content"`
	} `json:"articles"`
}

type AlphaVantageResponse struct {
	Feed []struct {
		Title            string `json:"title"`
		URL              string `json:"url"`
		TimePublished    string `json:"time_published"`
		Summary          string `json:"summary"`
		Source           string `json:"source"`
		OverallSentiment string `json:"overall_sentiment_label"`
		TickerSentiment  []struct {
			Ticker string `json:"ticker"`
		} `json:"ticker_sentiment"`
	} `json:"feed"`
}

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	config := loadConfig()

	producerConfig := &producer.Config{
		ProducerName:    "news-producer",
		Topic:           "news-producer",
		KafkaBrokers:    []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistry:  "http://schema-registry:8081",
		BatchSize:       50,
		FlushInterval:   1 * time.Second,
		RateLimitPerSec: 100,
		RetryAttempts:   5,
		RetryBackoff:    100 * time.Millisecond,
	}

	baseProducer, err := producer.NewBaseProducer(producerConfig, &zapLoggerAdapter{logger})
	if err != nil {
		logger.Fatalf("Error creating base producer: %v", err)
	}

	newsProducer := &NewsProducer{
		baseProducer:        baseProducer,
		config:              config,
		logger:              logger,
		httpClient:          &http.Client{Timeout: 30 * time.Second},
		newsAPILimiter:      rate.NewLimiter(rate.Limit(100), 100),
		alphaVantageLimiter: rate.NewLimiter(rate.Every(12*time.Second), 1), // 5 per minute
		newsAPIBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "newsapi",
			MaxRequests: 3,
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
		}),
		alphaVantageBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "alphavantage",
			MaxRequests: 3,
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
		}),
		dedupeCache: make(map[string]time.Time),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := newsProducer.Start(ctx); err != nil {
			logger.Errorf("Error starting news-producer: %v", err)
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down news-producer...")
	cancel()

	if err := newsProducer.Stop(); err != nil {
		logger.Errorf("Error stopping news-producer: %v", err)
		os.Exit(1)
	}

	logger.Info("News producer stopped successfully")
}

func (np *NewsProducer) Start(ctx context.Context) error {
	np.logger.Info("Starting news producer")

	newsAPITicker := time.NewTicker(np.config.NewsAPIInterval)
	alphaVantageTicker := time.NewTicker(np.config.AlphaVantageInterval)
	cleanupTicker := time.NewTicker(1 * time.Hour)

	defer newsAPITicker.Stop()
	defer alphaVantageTicker.Stop()
	defer cleanupTicker.Stop()

	// Initial fetch
	np.fetchNewsAPI(ctx)
	np.fetchAlphaVantage(ctx)

	for {
		select {
		case <-ctx.Done():
			np.logger.Info("Shutting down news producer...")
			return nil
		case <-newsAPITicker.C:
			np.fetchNewsAPI(ctx)
		case <-alphaVantageTicker.C:
			np.fetchAlphaVantage(ctx)
		case <-cleanupTicker.C:
			np.cleanupCache()
		}
	}
}

func (np *NewsProducer) fetchNewsAPI(ctx context.Context) {
	if err := np.newsAPILimiter.Wait(ctx); err != nil {
		np.logger.Errorf("Rate limiter error for NewsAPI: %v", err)
		return
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
	}

	err := middleware.Retry(ctx, retryConfig, func() error {
		return np.newsAPIBreaker.Execute(func() error {
			return np.fetchNewsAPIInternal(ctx)
		})
	})

	if err != nil {
		np.logger.Errorf("Failed to fetch from NewsAPI after retries: %v", err)
		np.baseProducer.IncrementMessageFailed("newsapi_fetch_error")
	}
}

func (np *NewsProducer) fetchNewsAPIInternal(ctx context.Context) error {
	url := fmt.Sprintf("%s?q=%s&apiKey=%s&language=en&sortBy=publishedAt",
		np.config.NewsAPIURL, np.config.Query, np.config.NewsAPIKey)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	startTime := time.Now()
	resp, err := np.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch news: %w", err)
	}
	defer resp.Body.Close()

	np.baseProducer.RecordAPICall("newsapi_fetch", time.Since(startTime))

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

	var newsResp NewsAPIResponse
	if err := json.Unmarshal(body, &newsResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	np.logger.Infof("Fetched %d articles from NewsAPI", len(newsResp.Articles))

	for _, article := range newsResp.Articles {
		newsArticle := NewsArticle{
			ID:          np.generateID(article.URL),
			Source:      "newsapi",
			Title:       article.Title,
			Description: article.Description,
			URL:         article.URL,
			PublishedAt: article.PublishedAt,
			Author:      article.Author,
			Content:     article.Content,
			Timestamp:   time.Now(),
		}

		if err := np.publishArticle(ctx, newsArticle); err != nil {
			np.logger.Errorf("Failed to publish article: %v", err)
		}
	}

	return nil
}

func (np *NewsProducer) fetchAlphaVantage(ctx context.Context) {
	if err := np.alphaVantageLimiter.Wait(ctx); err != nil {
		np.logger.Errorf("Rate limiter error for AlphaVantage: %v", err)
		return
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 3 * time.Second,
		MaxBackoff:     15 * time.Second,
		Multiplier:     2.0,
	}

	err := middleware.Retry(ctx, retryConfig, func() error {
		return np.alphaVantageBreaker.Execute(func() error {
			return np.fetchAlphaVantageInternal(ctx)
		})
	})

	if err != nil {
		np.logger.Errorf("Failed to fetch from AlphaVantage after retries: %v", err)
		np.baseProducer.IncrementMessageFailed("alphavantage_fetch_error")
	}
}

func (np *NewsProducer) fetchAlphaVantageInternal(ctx context.Context) error {
	url := fmt.Sprintf("%s?function=NEWS_SENTIMENT&apikey=%s&topics=finance&sort=LATEST",
		np.config.AlphaVantageURL, np.config.AlphaVantageKey)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	startTime := time.Now()
	resp, err := np.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch news: %w", err)
	}
	defer resp.Body.Close()

	np.baseProducer.RecordAPICall("alphavantage_fetch", time.Since(startTime))

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

	var avResp AlphaVantageResponse
	if err := json.Unmarshal(body, &avResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	np.logger.Infof("Fetched %d articles from AlphaVantage", len(avResp.Feed))

	for _, item := range avResp.Feed {
		publishedAt, _ := time.Parse("20060102T150405", item.TimePublished)

		symbols := make([]string, 0, len(item.TickerSentiment))
		for _, ts := range item.TickerSentiment {
			symbols = append(symbols, ts.Ticker)
		}

		newsArticle := NewsArticle{
			ID:          np.generateID(item.URL),
			Source:      "alphavantage",
			Title:       item.Title,
			Description: item.Summary,
			URL:         item.URL,
			PublishedAt: publishedAt,
			Sentiment:   item.OverallSentiment,
			Symbols:     symbols,
			Timestamp:   time.Now(),
		}

		if err := np.publishArticle(ctx, newsArticle); err != nil {
			np.logger.Errorf("Failed to publish article: %v", err)
		}
	}

	return nil
}

func (np *NewsProducer) publishArticle(ctx context.Context, article NewsArticle) error {
	// Deduplication check
	if lastSeen, exists := np.dedupeCache[article.ID]; exists {
		if time.Since(lastSeen) < np.config.DedupeWindow {
			np.logger.Debugf("Skipping duplicate article: %s", article.ID)
			return nil
		}
	}

	if err := np.validateArticle(&article); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	value, err := json.Marshal(article)
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := []byte(article.ID)

	startTime := time.Now()
	if err := np.baseProducer.PublishMessage(ctx, key, value); err != nil {
		np.baseProducer.IncrementMessageFailed("kafka_error")
		return fmt.Errorf("error publishing message: %w", err)
	}

	np.baseProducer.IncrementMessageSent(int64(len(value)))
	np.baseProducer.RecordAPICall("kafka_publish", time.Since(startTime))

	// Update dedupe cache
	np.dedupeCache[article.ID] = time.Now()

	return nil
}

func (np *NewsProducer) validateArticle(article *NewsArticle) error {
	if article.ID == "" {
		return fmt.Errorf("empty article ID")
	}

	if article.Title == "" {
		return fmt.Errorf("empty title")
	}

	if article.URL == "" {
		return fmt.Errorf("empty URL")
	}

	if article.PublishedAt.IsZero() {
		return fmt.Errorf("empty published_at timestamp")
	}

	return nil
}

func (np *NewsProducer) generateID(url string) string {
	hash := sha256.Sum256([]byte(url))
	return fmt.Sprintf("%x", hash[:8])
}

func (np *NewsProducer) cleanupCache() {
	now := time.Now()
	for id, timestamp := range np.dedupeCache {
		if now.Sub(timestamp) > np.config.DedupeWindow {
			delete(np.dedupeCache, id)
		}
	}
	np.logger.Infof("Cleaned up dedupe cache, %d entries remaining", len(np.dedupeCache))
}

func (np *NewsProducer) Stop() error {
	np.logger.Info("Stopping news producer...")
	return np.baseProducer.Stop()
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

func loadConfig() *NewsConfig {
	return &NewsConfig{
		NewsAPIKey:           getEnv("NEWS_API_KEY", ""),
		NewsAPIURL:           getEnv("NEWS_API_URL", "https://newsapi.org/v2/everything"),
		AlphaVantageKey:      getEnv("ALPHAVANTAGE_API_KEY", ""),
		AlphaVantageURL:      getEnv("ALPHAVANTAGE_URL", "https://www.alphavantage.co/query"),
		NewsAPIInterval:      60 * time.Second,
		AlphaVantageInterval: 300 * time.Second,
		DedupeWindow:         24 * time.Hour,
		Query:                getEnv("NEWS_QUERY", "finance OR stocks OR market"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
