package client

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"trendyol/internal/proxy"
)

func TestRotateRebuildsStickyTLSClient(t *testing.T) {
	pool := proxy.New(nil, "http://account-region-TR-sid-{sid}:secret@proxy.example:9000")
	session, err := NewFactory(pool, time.Second, 10, 0).NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	firstSID, firstProxy := session.SID(), session.ProxyRaw()
	if err := session.Rotate("http_429"); err != nil {
		t.Fatal(err)
	}
	if session.SID() == firstSID || session.ProxyRaw() == firstProxy {
		t.Fatal("Rotate must create a new SID and proxy-bound client")
	}
	if session.Pages() != 0 || !strings.Contains(session.ProxyRaw(), "region-TR") {
		t.Fatalf("rotation did not reset page count or preserve Turkish endpoint: pages=%d proxy=%s", session.Pages(), session.ProxyMasked())
	}
	if stats := pool.Snapshot(); stats.Failures != 1 || stats.Cooling != 1 {
		t.Fatalf("old exit was not cooled: %+v", stats)
	}
}

func TestSessionSeedsTurkeyStorefront(t *testing.T) {
	session, err := NewFactory(proxy.New(nil, ""), time.Second, 10, 0).NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	root, _ := url.Parse("https://www.trendyol.com/")
	values := map[string]string{}
	for _, cookie := range session.jar.Cookies(root) {
		values[cookie.Name] = cookie.Value
	}
	if values["storefrontId"] != "1" || values["countryCode"] != "TR" || values["language"] != "tr" {
		t.Fatalf("unexpected storefront cookies: %+v", values)
	}
}
