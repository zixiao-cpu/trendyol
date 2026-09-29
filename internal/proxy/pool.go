package proxy

import (
	"crypto/rand"
	"math/big"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var sidPattern = regexp.MustCompile(`(?i)sid-[A-Za-z0-9]+`)

type entry struct {
	base          string
	successes     atomic.Int64
	failures      atomic.Int64
	cooldownUntil atomic.Int64
}

type Pool struct {
	entries []*entry
	next    atomic.Uint64
	mu      sync.Mutex
}

type Stats struct {
	Total, Cooling, Successes, Failures int64
}

func New(urls []string, template string) *Pool {
	seen := map[string]bool{}
	if strings.TrimSpace(template) != "" {
		urls = append(urls, strings.TrimSpace(template))
	}
	p := &Pool{}
	for _, raw := range urls {
		raw = strings.TrimSpace(raw)
		if raw != "" && !seen[raw] {
			seen[raw] = true
			p.entries = append(p.entries, &entry{base: raw})
		}
	}
	return p
}

func (p *Pool) Next(sid string) string {
	if len(p.entries) == 0 {
		return ""
	}
	now := time.Now().UnixNano()
	start := int(p.next.Add(1)-1) % len(p.entries)
	var chosen *entry
	for index := range p.entries {
		entry := p.entries[(start+index)%len(p.entries)]
		if entry.cooldownUntil.Load() <= now {
			chosen = entry
			break
		}
	}
	if chosen == nil {
		chosen = p.entries[start]
	}
	return WithSID(chosen.base, sid)
}

func WithSID(raw, sid string) string {
	if strings.Contains(raw, "{sid}") {
		return strings.ReplaceAll(raw, "{sid}", sid)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		if sidPattern.MatchString(raw) {
			return sidPattern.ReplaceAllString(raw, "sid-"+sid)
		}
		return raw
	}
	username := parsed.User.Username()
	password, hasPassword := parsed.User.Password()
	if sidPattern.MatchString(username) {
		username = sidPattern.ReplaceAllString(username, "sid-"+sid)
	} else {
		username += "-sid-" + sid
	}
	if hasPassword {
		parsed.User = url.UserPassword(username, password)
	} else {
		parsed.User = url.User(username)
	}
	return parsed.String()
}

func (p *Pool) ReportSuccess(proxyURL string) {
	if entry := p.find(proxyURL); entry != nil {
		entry.successes.Add(1)
		entry.cooldownUntil.Store(0)
	}
}

func (p *Pool) ReportFailure(proxyURL string, reason string) {
	if entry := p.find(proxyURL); entry != nil {
		failures := entry.failures.Add(1)
		seconds := int64(10) << min(int(failures-1), 4)
		if strings.Contains(reason, "429") || strings.Contains(reason, "503") || strings.Contains(reason, "challenge") {
			seconds *= 2
		}
		if seconds > 300 {
			seconds = 300
		}
		entry.cooldownUntil.Store(time.Now().Add(time.Duration(seconds) * time.Second).UnixNano())
	}
}

func (p *Pool) find(proxyURL string) *entry {
	masked := Mask(proxyURL)
	for _, entry := range p.entries {
		if Mask(WithSID(entry.base, "healthcheck")) == masked {
			return entry
		}
	}
	return nil
}

func (p *Pool) Snapshot() Stats {
	stats := Stats{Total: int64(len(p.entries))}
	now := time.Now().UnixNano()
	for _, entry := range p.entries {
		stats.Successes += entry.successes.Load()
		stats.Failures += entry.failures.Load()
		if entry.cooldownUntil.Load() > now {
			stats.Cooling++
		}
	}
	return stats
}

func Mask(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "proxy"
	}
	return parsed.Scheme + "://" + parsed.Host
}

func NewSID(length int) string {
	if length < 6 {
		length = 10
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for index := range result {
		value, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			result[index] = alphabet[index%len(alphabet)]
		} else {
			result[index] = alphabet[value.Int64()]
		}
	}
	return string(result)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
