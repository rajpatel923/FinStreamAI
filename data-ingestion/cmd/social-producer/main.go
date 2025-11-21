package socialproducer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/FinStreamAl/data-ingestion/internal/middleware"
	"github.com/FinStreamAl/data-ingestion/internal/producer"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type SocialProducer struct {
	baseProducer      *producer.BaseProducer
	httpClient        *http.Client
	config            *SocialConfig
	logger            *zap.SugaredLogger
	twitterLimiter    *rate.Limiter
	redditLimiter     *rate.Limiter
	twitterBreaker    *middleware.CircuitBreaker
	redditBreaker     *middleware.CircuitBreaker
	redditToken       string
	redditTokenExpiry time.Time
	tokenMutex        sync.RWMutex
}

type SocialConfig struct {
	TwitterBearerToken string
	TwitterStreamURL   string
	RedditClientID     string
	RedditClientSecret string
	RedditUserAgent    string
	RedditOAuthURL     string
	RedditAPIURL       string
	Subreddits         []string
	RedditInterval     time.Duration
	TwitterRulesURL    string
	StreamRules        []string
}

type SocialPost struct {
	ID        string    `json:"id"`
	Platform  string    `json:"platform"`
	Author    string    `json:"author"`
	Content   string    `json:"content"`
	URL       string    `json:"url,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	Likes     int64     `json:"likes,omitempty"`
	Retweets  int64     `json:"retweets,omitempty"`
	Comments  int64     `json:"comments,omitempty"`
	Symbols   []string  `json:"symbols,omitempty"`
	Hashtags  []string  `json:"hashtags,omitempty"`
	Subreddit string    `json:"subreddit,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type TwitterStreamResponse struct {
	Data struct {
		ID        string    `json:"id"`
		Text      string    `json:"text"`
		CreatedAt time.Time `json:"created_at"`
		AuthorID  string    `json:"author_id"`
	} `json:"data"`
	Includes struct {
		Users []struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"users"`
	} `json:"includes,omitempty"`
}

type RedditAuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type RedditListingResponse struct {
	Data struct {
		Children []struct {
			Data struct {
				ID            string  `json:"id"`
				Title         string  `json:"title"`
				Selftext      string  `json:"selftext"`
				Author        string  `json:"author"`
				Subreddit     string  `json:"subreddit"`
				CreatedUTC    float64 `json:"created_utc"`
				Score         int64   `json:"score"`
				NumComments   int64   `json:"num_comments"`
				URL           string  `json:"url"`
				LinkFlairText string  `json:"link_flair_text"`
			} `json:"data"`
		} `json:"children"`
	} `json:"data"`
}

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	config := loadConfig()

	producerConfig := &producer.Config{
		ProducerName:    "social-producer",
		Topic:           "social-producer",
		KafkaBrokers:    []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistry:  "http://schema-registry:8081",
		BatchSize:       50,
		FlushInterval:   1 * time.Second,
		RateLimitPerSec: 1000,
		RetryAttempts:   5,
		RetryBackoff:    100 * time.Millisecond,
	}

	baseProducer, err := producer.NewBaseProducer(producerConfig, &zapLoggerAdapter{logger})
	if err != nil {
		logger.Fatalf("Error creating base producer: %v", err)
	}

	socialProducer := &SocialProducer{
		baseProducer:   baseProducer,
		config:         config,
		logger:         logger,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		twitterLimiter: rate.NewLimiter(rate.Limit(1000), 1000),
		redditLimiter:  rate.NewLimiter(rate.Every(time.Minute), 60), // 60 per minute
		twitterBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "twitter",
			MaxRequests: 5,
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
		}),
		redditBreaker: middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
			Name:        "reddit",
			MaxRequests: 5,
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := socialProducer.Start(ctx); err != nil {
			logger.Errorf("Error starting social-producer: %v", err)
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down social-producer...")
	cancel()

	if err := socialProducer.Stop(); err != nil {
		logger.Errorf("Error stopping social-producer: %v", err)
		os.Exit(1)
	}

	logger.Info("Social producer stopped successfully")
}

func (sp *SocialProducer) Start(ctx context.Context) error {
	sp.logger.Info("Starting social producer")

	// Authenticate with Reddit
	if err := sp.authenticateReddit(ctx); err != nil {
		sp.logger.Errorf("Failed to authenticate with Reddit: %v", err)
	}

	var wg sync.WaitGroup

	// Start Twitter streaming in separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.streamTwitter(ctx)
	}()

	// Start Reddit polling in separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp.pollReddit(ctx)
	}()

	wg.Wait()
	return nil
}

func (sp *SocialProducer) streamTwitter(ctx context.Context) {
	sp.logger.Info("Starting Twitter streaming")

	for {
		select {
		case <-ctx.Done():
			sp.logger.Info("Stopping Twitter streaming...")
			return
		default:
			retryConfig := middleware.RetryConfig{
				MaxAttempts:    3,
				InitialBackoff: 5 * time.Second,
				MaxBackoff:     30 * time.Second,
				Multiplier:     2.0,
			}

			err := middleware.Retry(ctx, retryConfig, func() error {
				return sp.twitterBreaker.Execute(func() error {
					return sp.streamTwitterInternal(ctx)
				})
			})

			if err != nil {
				sp.logger.Errorf("Twitter streaming error: %v, reconnecting...", err)
				sp.baseProducer.IncrementMessageFailed("twitter_stream_error")
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (sp *SocialProducer) streamTwitterInternal(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", sp.config.TwitterStreamURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", sp.config.TwitterBearerToken))

	resp, err := sp.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Twitter stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	sp.logger.Info("Connected to Twitter streaming API")

	decoder := json.NewDecoder(resp.Body)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := sp.twitterLimiter.Wait(ctx); err != nil {
				return err
			}

			var twitterMsg TwitterStreamResponse
			if err := decoder.Decode(&twitterMsg); err != nil {
				if err == io.EOF {
					return fmt.Errorf("stream ended: %w", err)
				}
				sp.logger.Errorf("Error decoding Twitter message: %v", err)
				continue
			}

			if twitterMsg.Data.ID == "" {
				continue
			}

			if err := sp.processTwitterPost(ctx, twitterMsg); err != nil {
				sp.logger.Errorf("Error processing Twitter post: %v", err)
			}
		}
	}
}

func (sp *SocialProducer) processTwitterPost(ctx context.Context, twitterMsg TwitterStreamResponse) error {
	username := twitterMsg.Data.AuthorID
	if len(twitterMsg.Includes.Users) > 0 {
		username = twitterMsg.Includes.Users[0].Username
	}

	post := SocialPost{
		ID:        sp.generateID(twitterMsg.Data.ID),
		Platform:  "twitter",
		Author:    username,
		Content:   twitterMsg.Data.Text,
		CreatedAt: twitterMsg.Data.CreatedAt,
		Symbols:   extractCashtags(twitterMsg.Data.Text),
		Hashtags:  extractHashtags(twitterMsg.Data.Text),
		Timestamp: time.Now(),
	}

	return sp.publishPost(ctx, post)
}

func (sp *SocialProducer) pollReddit(ctx context.Context) {
	sp.logger.Info("Starting Reddit polling")

	ticker := time.NewTicker(sp.config.RedditInterval)
	defer ticker.Stop()

	// Initial fetch
	sp.fetchReddit(ctx)

	for {
		select {
		case <-ctx.Done():
			sp.logger.Info("Stopping Reddit polling...")
			return
		case <-ticker.C:
			sp.fetchReddit(ctx)
		}
	}
}

func (sp *SocialProducer) fetchReddit(ctx context.Context) {
	if err := sp.redditLimiter.Wait(ctx); err != nil {
		sp.logger.Errorf("Rate limiter error for Reddit: %v", err)
		return
	}

	// Check if token needs refresh
	sp.tokenMutex.RLock()
	needsRefresh := time.Now().After(sp.redditTokenExpiry)
	sp.tokenMutex.RUnlock()

	if needsRefresh {
		if err := sp.authenticateReddit(ctx); err != nil {
			sp.logger.Errorf("Failed to refresh Reddit token: %v", err)
			return
		}
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
	}

	err := middleware.Retry(ctx, retryConfig, func() error {
		return sp.redditBreaker.Execute(func() error {
			return sp.fetchRedditInternal(ctx)
		})
	})

	if err != nil {
		sp.logger.Errorf("Failed to fetch from Reddit after retries: %v", err)
		sp.baseProducer.IncrementMessageFailed("reddit_fetch_error")
	}
}

func (sp *SocialProducer) fetchRedditInternal(ctx context.Context) error {
	for _, subreddit := range sp.config.Subreddits {
		url := fmt.Sprintf("%s/r/%s/new.json?limit=25", sp.config.RedditAPIURL, subreddit)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		sp.tokenMutex.RLock()
		token := sp.redditToken
		sp.tokenMutex.RUnlock()

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("User-Agent", sp.config.RedditUserAgent)

		startTime := time.Now()
		resp, err := sp.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to fetch from Reddit: %w", err)
		}
		defer resp.Body.Close()

		sp.baseProducer.RecordAPICall("reddit_fetch", time.Since(startTime))

		if resp.StatusCode == 429 {
			return fmt.Errorf("rate limit exceeded")
		}

		if resp.StatusCode == 401 {
			sp.tokenMutex.Lock()
			sp.redditTokenExpiry = time.Now()
			sp.tokenMutex.Unlock()
			return fmt.Errorf("authentication expired")
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		var listing RedditListingResponse
		if err := json.Unmarshal(body, &listing); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		sp.logger.Infof("Fetched %d posts from r/%s", len(listing.Data.Children), subreddit)

		for _, child := range listing.Data.Children {
			post := SocialPost{
				ID:        sp.generateID(child.Data.ID),
				Platform:  "reddit",
				Author:    child.Data.Author,
				Content:   fmt.Sprintf("%s\n%s", child.Data.Title, child.Data.Selftext),
				URL:       child.Data.URL,
				CreatedAt: time.Unix(int64(child.Data.CreatedUTC), 0),
				Likes:     child.Data.Score,
				Comments:  child.Data.NumComments,
				Subreddit: child.Data.Subreddit,
				Symbols:   extractCashtags(child.Data.Title + " " + child.Data.Selftext),
				Timestamp: time.Now(),
			}

			if err := sp.publishPost(ctx, post); err != nil {
				sp.logger.Errorf("Failed to publish Reddit post: %v", err)
			}
		}
	}

	return nil
}

func (sp *SocialProducer) authenticateReddit(ctx context.Context) error {
	sp.logger.Info("Authenticating with Reddit...")

	data := url.Values{}
	data.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, "POST", sp.config.RedditOAuthURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create auth request: %w", err)
	}

	req.SetBasicAuth(sp.config.RedditClientID, sp.config.RedditClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", sp.config.RedditUserAgent)

	resp, err := sp.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to authenticate: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	var authResp RedditAuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return fmt.Errorf("failed to parse auth response: %w", err)
	}

	sp.tokenMutex.Lock()
	sp.redditToken = authResp.AccessToken
	sp.redditTokenExpiry = time.Now().Add(time.Duration(authResp.ExpiresIn) * time.Second)
	sp.tokenMutex.Unlock()

	sp.logger.Info("Successfully authenticated with Reddit")
	return nil
}

func (sp *SocialProducer) publishPost(ctx context.Context, post SocialPost) error {
	if err := sp.validatePost(&post); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	value, err := json.Marshal(post)
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := []byte(post.ID)

	startTime := time.Now()
	if err := sp.baseProducer.PublishMessage(ctx, key, value); err != nil {
		sp.baseProducer.IncrementMessageFailed("kafka_error")
		return fmt.Errorf("error publishing message: %w", err)
	}

	sp.baseProducer.IncrementMessageSent(int64(len(value)))
	sp.baseProducer.RecordAPICall("kafka_publish", time.Since(startTime))

	return nil
}

func (sp *SocialProducer) validatePost(post *SocialPost) error {
	if post.ID == "" {
		return fmt.Errorf("empty post ID")
	}

	if post.Platform == "" {
		return fmt.Errorf("empty platform")
	}

	if post.Content == "" {
		return fmt.Errorf("empty content")
	}

	if post.CreatedAt.IsZero() {
		return fmt.Errorf("empty created_at timestamp")
	}

	return nil
}

func (sp *SocialProducer) generateID(rawID string) string {
	hash := sha256.Sum256([]byte(rawID))
	return fmt.Sprintf("%x", hash[:8])
}

func extractCashtags(text string) []string {
	var cashtags []string
	words := strings.Fields(text)
	for _, word := range words {
		if strings.HasPrefix(word, "$") && len(word) > 1 {
			cashtag := strings.TrimPrefix(word, "$")
			cashtag = strings.TrimRight(cashtag, ".,!?;:")
			if len(cashtag) > 0 && len(cashtag) <= 5 {
				cashtags = append(cashtags, strings.ToUpper(cashtag))
			}
		}
	}
	return cashtags
}

func extractHashtags(text string) []string {
	var hashtags []string
	words := strings.Fields(text)
	for _, word := range words {
		if strings.HasPrefix(word, "#") && len(word) > 1 {
			hashtag := strings.TrimPrefix(word, "#")
			hashtag = strings.TrimRight(hashtag, ".,!?;:")
			if len(hashtag) > 0 {
				hashtags = append(hashtags, hashtag)
			}
		}
	}
	return hashtags
}

func (sp *SocialProducer) Stop() error {
	sp.logger.Info("Stopping social producer...")
	return sp.baseProducer.Stop()
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

func loadConfig() *SocialConfig {
	return &SocialConfig{
		TwitterBearerToken: getEnv("TWITTER_BEARER_TOKEN", ""),
		TwitterStreamURL:   getEnv("TWITTER_STREAM_URL", "https://api.twitter.com/2/tweets/search/stream"),
		RedditClientID:     getEnv("REDDIT_CLIENT_ID", ""),
		RedditClientSecret: getEnv("REDDIT_CLIENT_SECRET", ""),
		RedditUserAgent:    getEnv("REDDIT_USER_AGENT", "FinStreamAI/1.0"),
		RedditOAuthURL:     getEnv("REDDIT_OAUTH_URL", "https://www.reddit.com/api/v1/access_token"),
		RedditAPIURL:       getEnv("REDDIT_API_URL", "https://oauth.reddit.com"),
		Subreddits:         []string{"wallstreetbets", "stocks", "investing"},
		RedditInterval:     30 * time.Second,
		TwitterRulesURL:    getEnv("TWITTER_RULES_URL", "https://api.twitter.com/2/tweets/search/stream/rules"),
		StreamRules:        []string{"$AAPL OR $GOOGL OR $MSFT", "cashtag lang:en"},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
