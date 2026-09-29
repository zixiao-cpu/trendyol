package bypass

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

var ErrDisabled = errors.New("cloudbypass disabled or budget exhausted")

type Stats struct {
	Attempts, Successes, Failures int64
	Spent                         float64
}

type Client struct {
	mu      sync.Mutex
	enabled bool
	apiKey  string
	budget  float64
	stats   Stats
	http    tlsclient.HttpClient
}

type Result struct {
	Status, APIStatus int
	Body              []byte
	Duration          time.Duration
	Cost              float64
}

func New(enabled bool, apiKey string, timeout time.Duration, budget float64) (*Client, error) {
	client, err := tlsclient.NewHttpClient(tlsclient.NewNoopLogger(), tlsclient.WithTimeoutSeconds(int(timeout.Seconds())), tlsclient.WithClientProfile(profiles.Chrome_131), tlsclient.WithCookieJar(tlsclient.NewCookieJar()))
	if err != nil {
		return nil, err
	}
	return &Client{enabled: enabled, apiKey: apiKey, budget: budget, http: client}, nil
}

func (c *Client) Fetch(ctx context.Context, path, proxyURL string) (*Result, error) {
	const cost = 3.0
	if !c.reserve(cost) {
		return nil, ErrDisabled
	}
	started := time.Now()
	request, err := fhttp.NewRequestWithContext(ctx, fhttp.MethodGet, "https://api.cloudbypass.com"+path, nil)
	if err != nil {
		c.finish(false)
		return &Result{Duration: time.Since(started), Cost: cost}, err
	}
	request.Header = fhttp.Header{
		"x-cb-apikey":     {c.apiKey},
		"x-cb-host":       {"www.trendyol.com"},
		"x-cb-protocol":   {"https"},
		"x-cb-version":    {"2"},
		"user-agent":      {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"},
		"accept":          {"text/html,application/json,*/*"},
		"accept-language": {"tr-TR,tr;q=0.9"},
	}
	if proxyURL != "" {
		request.Header.Set("x-cb-proxy", proxyURL)
	}
	response, err := c.http.Do(request)
	if err != nil {
		c.finish(false)
		return &Result{Duration: time.Since(started), Cost: cost}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 12<<20))
	if err != nil {
		c.finish(false)
		return &Result{Status: response.StatusCode, APIStatus: response.StatusCode, Duration: time.Since(started), Cost: cost}, err
	}
	success := response.StatusCode == 200
	c.finish(success)
	return &Result{Status: response.StatusCode, APIStatus: response.StatusCode, Body: body, Duration: time.Since(started), Cost: cost}, nil
}

func (c *Client) reserve(cost float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled || c.apiKey == "" || (c.budget > 0 && c.stats.Spent+cost > c.budget) {
		return false
	}
	c.stats.Attempts++
	c.stats.Spent += cost
	return true
}

func (c *Client) finish(success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if success {
		c.stats.Successes++
	} else {
		c.stats.Failures++
	}
}

func (c *Client) Snapshot() Stats { c.mu.Lock(); defer c.mu.Unlock(); return c.stats }
func (c *Client) SetPersistedSpent(spent float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if spent > c.stats.Spent {
		c.stats.Spent = spent
	}
}
func (c *Client) Close() {
	if c.http != nil {
		c.http.CloseIdleConnections()
	}
}
func (c *Client) String() string { return fmt.Sprintf("enabled=%v budget=%.2f", c.enabled, c.budget) }
