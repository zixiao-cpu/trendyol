package client

import (
	"time"

	"trendyol/internal/proxy"
)

type Factory struct {
	pool         *proxy.Pool
	timeout      time.Duration
	stickyPages  int
	stickyJitter int
}

func NewFactory(pool *proxy.Pool, timeout time.Duration, stickyPages, stickyJitter int) *Factory {
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	if stickyPages < 1 {
		stickyPages = 24
	}
	if stickyJitter < 0 {
		stickyJitter = 0
	}
	return &Factory{pool: pool, timeout: timeout, stickyPages: stickyPages, stickyJitter: stickyJitter}
}

func (f *Factory) NewSession() (*Session, error) {
	session := &Session{pool: f.pool, timeout: f.timeout, basePages: f.stickyPages, pageJitter: f.stickyJitter}
	if err := session.Rotate("initial"); err != nil {
		return nil, err
	}
	return session, nil
}

func (f *Factory) Pool() *proxy.Pool { return f.pool }
