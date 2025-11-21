package secproducer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
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

type SECProducer struct {
	baseProducer   *producer.BaseProducer
	httpClient     *http.Client
	config         *SECConfig
	logger         *zap.SugaredLogger
	rateLimiter    *rate.Limiter
	circuitBreaker *middleware.CircuitBreaker
	dedupeCache    map[string]time.Time
}

type SECConfig struct {
	RSSURL       string
	UserAgent    string
	FormTypes    []string
	PollInterval time.Duration
	DedupeWindow time.Duration
}

type SECFiling struct {
	AccessionNumber string    `json:"accession_number"`
	FormType        string    `json:"form_type"`
	CompanyName     string    `json:"company_name"`
	CIK             string    `json:"cik"`
	FilingDate      time.Time `json:"filing_date"`
	FilingURL       string    `json:"filing_url"`
	Description     string    `json:"description,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
}

type RSSFeed struct {
	XMLName xml.Name   `xml:"rss"`
	Channel RSSChannel `xml:"channel"`
}

type RSSChannel struct {
	Title       string    `xml:"title"`
	Link        string    `xml:"link"`
	Description string    `xml:"description"`
	Items       []RSSItem `xml:"item"`
}

type RSSItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	GUID        string `xml:"guid"`
}

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync()
	logger := zapLogger.Sugar()

	config := loadConfig()

	producerConfig := &producer.Config{
		ProducerName:    "sec-filing-producer",
		Topic:           "sec-filing-producer",
		KafkaBrokers:    []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistry:  "http://schema-registry:8081",
		BatchSize:       20,
		FlushInterval:   5 * time.Second,
		RateLimitPerSec: 10,
		RetryAttempts:   5,
		RetryBackoff:    100 * time.Millisecond,
	}

	baseProducer, err := producer.NewBaseProducer(producerConfig, &zapLoggerAdapter{logger})
	if err != nil {
		logger.Fatalf("Error creating base producer: %v", err)
	}

	secProducer := &SECProducer{
		baseProducer: baseProducer,
		config:       config,
		logger:       logger,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		rateLimiter:  rate.NewLimiter(rate.Limit(10), 10), // 10 req/sec
		//circuitBreaker: middleware.NewCircuitBreaker("sec", 3, 30*time.Second, 60*time.Second),
		dedupeCache: make(map[string]time.Time),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := secProducer.Start(ctx); err != nil {
			logger.Errorf("Error starting sec-filing-producer: %v", err)
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down sec-filing-producer...")
	cancel()

	if err := secProducer.Stop(); err != nil {
		logger.Errorf("Error stopping sec-filing-producer: %v", err)
		os.Exit(1)
	}

	logger.Info("SEC filing producer stopped successfully")
}

func (sp *SECProducer) Start(ctx context.Context) error {
	sp.logger.Info("Starting SEC filing producer")

	ticker := time.NewTicker(sp.config.PollInterval)
	cleanupTicker := time.NewTicker(1 * time.Hour)

	defer ticker.Stop()
	defer cleanupTicker.Stop()

	// Initial fetch
	sp.fetchFilings(ctx)

	for {
		select {
		case <-ctx.Done():
			sp.logger.Info("Shutting down SEC filing producer...")
			return nil
		case <-ticker.C:
			sp.fetchFilings(ctx)
		case <-cleanupTicker.C:
			sp.cleanupCache()
		}
	}
}

func (sp *SECProducer) fetchFilings(ctx context.Context) {
	if err := sp.rateLimiter.Wait(ctx); err != nil {
		sp.logger.Errorf("Rate limiter error: %v", err)
		return
	}

	retryConfig := middleware.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
	}

	err := middleware.Retry(ctx, retryConfig, func() error {
		return sp.circuitBreaker.Execute(func() error {
			return sp.fetchFilingsInternal(ctx)
		})
	})

	if err != nil {
		sp.logger.Errorf("Failed to fetch SEC filings after retries: %v", err)
		sp.baseProducer.IncrementMessageFailed("sec_fetch_error")
	}
}

func (sp *SECProducer) fetchFilingsInternal(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", sp.config.RSSURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", sp.config.UserAgent)

	startTime := time.Now()
	resp, err := sp.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch RSS feed: %w", err)
	}
	defer resp.Body.Close()

	sp.baseProducer.RecordAPICall("sec_fetch", time.Since(startTime))

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

	var feed RSSFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return fmt.Errorf("failed to parse RSS feed: %w", err)
	}

	sp.logger.Infof("Fetched %d items from SEC EDGAR RSS", len(feed.Channel.Items))

	for _, item := range feed.Channel.Items {
		filing := sp.parseRSSItem(item)
		if filing == nil {
			continue
		}

		// Filter by form type
		if !sp.isFormTypeAllowed(filing.FormType) {
			continue
		}

		if err := sp.publishFiling(ctx, *filing); err != nil {
			sp.logger.Errorf("Failed to publish filing: %v", err)
		}
	}

	return nil
}

func (sp *SECProducer) parseRSSItem(item RSSItem) *SECFiling {
	// Parse title to extract form type and company name
	// Example title: "8-K - Apple Inc. (0000320193)"
	parts := strings.SplitN(item.Title, " - ", 2)
	if len(parts) < 2 {
		sp.logger.Debugf("Could not parse title: %s", item.Title)
		return nil
	}

	formType := strings.TrimSpace(parts[0])

	// Extract company name and CIK
	companyPart := parts[1]
	companyName := companyPart
	cik := ""

	// Try to extract CIK from parentheses
	if startIdx := strings.LastIndex(companyPart, "("); startIdx != -1 {
		if endIdx := strings.LastIndex(companyPart, ")"); endIdx > startIdx {
			cik = strings.TrimSpace(companyPart[startIdx+1 : endIdx])
			companyName = strings.TrimSpace(companyPart[:startIdx])
		}
	}

	// Parse pub date
	pubDate, err := time.Parse(time.RFC1123Z, item.PubDate)
	if err != nil {
		// Try alternative format
		pubDate, err = time.Parse(time.RFC1123, item.PubDate)
		if err != nil {
			sp.logger.Debugf("Could not parse date %s: %v", item.PubDate, err)
			pubDate = time.Now()
		}
	}

	// Extract accession number from link or GUID
	accessionNumber := sp.extractAccessionNumber(item.Link)
	if accessionNumber == "" {
		accessionNumber = sp.generateID(item.GUID)
	}

	return &SECFiling{
		AccessionNumber: accessionNumber,
		FormType:        formType,
		CompanyName:     companyName,
		CIK:             cik,
		FilingDate:      pubDate,
		FilingURL:       item.Link,
		Description:     item.Description,
		Timestamp:       time.Now(),
	}
}

func (sp *SECProducer) extractAccessionNumber(url string) string {
	// SEC URLs typically contain accession numbers like: 0000320193-24-000123
	// Example: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=8-K&dateb=&owner=exclude&count=100&search_text=
	// Or document URLs: https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/filename.htm

	parts := strings.Split(url, "/")
	for _, part := range parts {
		// Look for pattern like: 000032019324000123 (no dashes) or 0000320193-24-000123 (with dashes)
		if len(part) >= 18 && strings.Contains(part, "0000") {
			// Format with dashes if not present
			if !strings.Contains(part, "-") && len(part) == 18 {
				return part[:10] + "-" + part[10:12] + "-" + part[12:]
			}
			return part
		}
	}
	return ""
}

func (sp *SECProducer) isFormTypeAllowed(formType string) bool {
	for _, allowed := range sp.config.FormTypes {
		if strings.EqualFold(formType, allowed) {
			return true
		}
	}
	return false
}

func (sp *SECProducer) publishFiling(ctx context.Context, filing SECFiling) error {
	// Deduplication check
	if lastSeen, exists := sp.dedupeCache[filing.AccessionNumber]; exists {
		if time.Since(lastSeen) < sp.config.DedupeWindow {
			sp.logger.Debugf("Skipping duplicate filing: %s", filing.AccessionNumber)
			return nil
		}
	}

	if err := sp.validateFiling(&filing); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	value, err := json.Marshal(filing)
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := []byte(filing.AccessionNumber)

	startTime := time.Now()
	if err := sp.baseProducer.PublishMessage(ctx, key, value); err != nil {
		sp.baseProducer.IncrementMessageFailed("kafka_error")
		return fmt.Errorf("error publishing message: %w", err)
	}

	sp.baseProducer.IncrementMessageSent(int64(len(value)))
	sp.baseProducer.RecordAPICall("kafka_publish", time.Since(startTime))

	// Update dedupe cache
	sp.dedupeCache[filing.AccessionNumber] = time.Now()

	sp.logger.Infof("Published %s filing for %s", filing.FormType, filing.CompanyName)

	return nil
}

func (sp *SECProducer) validateFiling(filing *SECFiling) error {
	if filing.AccessionNumber == "" {
		return fmt.Errorf("empty accession number")
	}

	if filing.FormType == "" {
		return fmt.Errorf("empty form type")
	}

	if filing.CompanyName == "" {
		return fmt.Errorf("empty company name")
	}

	if filing.FilingURL == "" {
		return fmt.Errorf("empty filing URL")
	}

	if filing.FilingDate.IsZero() {
		return fmt.Errorf("empty filing date")
	}

	return nil
}

func (sp *SECProducer) generateID(guid string) string {
	hash := sha256.Sum256([]byte(guid))
	return fmt.Sprintf("%x", hash[:8])
}

func (sp *SECProducer) cleanupCache() {
	now := time.Now()
	for id, timestamp := range sp.dedupeCache {
		if now.Sub(timestamp) > sp.config.DedupeWindow {
			delete(sp.dedupeCache, id)
		}
	}
	sp.logger.Infof("Cleaned up dedupe cache, %d entries remaining", len(sp.dedupeCache))
}

func (sp *SECProducer) Stop() error {
	sp.logger.Info("Stopping SEC filing producer...")
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

func loadConfig() *SECConfig {
	return &SECConfig{
		RSSURL:       getEnv("SEC_RSS_URL", "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=exclude&start=0&count=100&output=atom"),
		UserAgent:    getEnv("SEC_USER_AGENT", "FinStreamAI bot@finstreami.com"),
		FormTypes:    []string{"8-K", "10-K", "10-Q", "13F", "4", "S-1"},
		PollInterval: 300 * time.Second, // 5 minutes
		DedupeWindow: 24 * time.Hour,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
