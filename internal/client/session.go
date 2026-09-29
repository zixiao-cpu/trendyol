package client

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"

	"trendyol/internal/proxy"
)

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

type Result struct {
	Status   int
	Body     []byte
	URL      string
	Bytes    int64
	Duration time.Duration
	ProxyKey string
}

type Session struct {
	mu         sync.Mutex
	pool       *proxy.Pool
	timeout    time.Duration
	basePages  int
	pageJitter int
	maxPages   int
	pages      int
	sid        string
	proxyURL   string
	http       tlsclient.HttpClient
	jar        tlsclient.CookieJar
}

func (s *Session) Rotate(reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotateLocked(reason)
}

func (s *Session) rotateLocked(reason string) error {
	if s.http != nil {
		s.http.CloseIdleConnections()
	}
	if s.pool != nil && s.proxyURL != "" && reason != "initial" && reason != "sticky_limit" {
		s.pool.ReportFailure(s.proxyURL, reason)
	}
	sid := proxy.NewSID(12)
	proxyURL := ""
	if s.pool != nil {
		proxyURL = s.pool.Next(sid)
	}
	jar := tlsclient.NewCookieJar()
	seedTurkeyStorefront(jar)
	options := []tlsclient.HttpClientOption{
		tlsclient.WithTimeoutSeconds(int(s.timeout.Seconds())),
		tlsclient.WithClientProfile(profiles.Chrome_131),
		tlsclient.WithCookieJar(jar),
		tlsclient.WithNotFollowRedirects(),
	}
	if proxyURL != "" {
		options = append(options, tlsclient.WithProxyUrl(proxyURL))
	}
	httpClient, err := tlsclient.NewHttpClient(tlsclient.NewNoopLogger(), options...)
	if err != nil {
		return fmt.Errorf("create tls client: %w", err)
	}
	s.sid, s.proxyURL, s.jar, s.http, s.pages = sid, proxyURL, jar, httpClient, 0
	s.maxPages = s.basePages
	if s.pageJitter > 0 {
		s.maxPages += rand.Intn(s.pageJitter + 1)
	}
	return nil
}

// Trendyol sends foreign exits to /en/select-country until the browser has
// selected a storefront. Seed the same first-party values written by the
// public country selector so every sticky worker session starts in Türkiye.
func seedTurkeyStorefront(jar tlsclient.CookieJar) {
	root, err := url.Parse("https://www.trendyol.com/")
	if err != nil {
		return
	}
	jar.SetCookies(root, []*fhttp.Cookie{
		{Name: "storefrontId", Value: "1", Path: "/", Domain: ".trendyol.com", Secure: true, SameSite: fhttp.SameSiteLaxMode},
		{Name: "countryCode", Value: "TR", Path: "/", Domain: ".trendyol.com", Secure: true, SameSite: fhttp.SameSiteLaxMode},
		{Name: "language", Value: "tr", Path: "/", Domain: ".trendyol.com", Secure: true, SameSite: fhttp.SameSiteLaxMode},
	})
}

func (s *Session) Get(ctx context.Context, rawURL string) (*Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http == nil || s.pages >= s.maxPages {
		if err := s.rotateLocked("sticky_limit"); err != nil {
			return nil, err
		}
	}
	request, err := fhttp.NewRequestWithContext(ctx, fhttp.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	setHeaders(request)
	started := time.Now()
	response, err := s.http.Do(request)
	s.pages++
	if err != nil {
		_ = s.rotateLocked("proxy_connection")
		return nil, err
	}
	for redirects := 0; redirects < 5 && response.StatusCode >= 300 && response.StatusCode < 400; redirects++ {
		location := response.Header.Get("Location")
		if location == "" {
			break
		}
		base, _ := url.Parse(rawURL)
		next, err := url.Parse(location)
		if err != nil {
			break
		}
		rawURL = base.ResolveReference(next).String()
		_ = response.Body.Close()
		request, err = fhttp.NewRequestWithContext(ctx, fhttp.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		setHeaders(request)
		response, err = s.http.Do(request)
		if err != nil {
			_ = s.rotateLocked("proxy_connection")
			return nil, err
		}
	}
	defer response.Body.Close()
	// Product pages are small, but Trendyol product sitemaps can exceed 12 MiB.
	// Keep a bounded limit while allowing a complete current sitemap response.
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		_ = s.rotateLocked("proxy_connection")
		return nil, err
	}
	if response.StatusCode == 200 && s.pool != nil {
		s.pool.ReportSuccess(s.proxyURL)
	}
	return &Result{Status: response.StatusCode, Body: body, URL: rawURL, Bytes: int64(len(body)), Duration: time.Since(started), ProxyKey: proxy.Mask(s.proxyURL)}, nil
}

func setHeaders(request *fhttp.Request) {
	request.Header = fhttp.Header{
		"User-Agent":                {userAgent},
		"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
		"Accept-Language":           {"tr-TR,tr;q=0.9,en;q=0.7"},
		"Accept-Encoding":           {"gzip, deflate, br"},
		"Cache-Control":             {"max-age=0"},
		"Upgrade-Insecure-Requests": {"1"},
		"Sec-Fetch-Dest":            {"document"},
		"Sec-Fetch-Mode":            {"navigate"},
		"Sec-Fetch-Site":            {"none"},
		"Sec-Fetch-User":            {"?1"},
		"Sec-Ch-Ua":                 {`"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`},
		"Sec-Ch-Ua-Mobile":          {"?0"},
		"Sec-Ch-Ua-Platform":        {`"Windows"`},
	}
	request.Header[fhttp.HeaderOrderKey] = []string{"host", "connection", "cache-control", "upgrade-insecure-requests", "user-agent", "accept", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-user", "sec-fetch-dest", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "accept-encoding", "accept-language", "cookie"}
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http != nil {
		s.http.CloseIdleConnections()
		s.http = nil
	}
	s.jar = nil
}

func (s *Session) SID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.sid }
func (s *Session) Pages() int  { s.mu.Lock(); defer s.mu.Unlock(); return s.pages }
func (s *Session) ProxyMasked() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proxy.Mask(s.proxyURL)
}
func (s *Session) ProxyRaw() string { s.mu.Lock(); defer s.mu.Unlock(); return s.proxyURL }
