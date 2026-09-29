package trendyol

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trendyol/internal/client"
	"trendyol/internal/store"
)

type fakeDiscoverySession struct {
	response *client.Result
	calls    int
}

func (f *fakeDiscoverySession) Get(context.Context, string) (*client.Result, error) {
	f.calls++
	return f.response, nil
}
func (f *fakeDiscoverySession) Rotate(string) error { return nil }

func TestImportURLsSeparatesProductsAndListingPages(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "urls.txt")
	content := "https://www.trendyol.com/a-p-881465224\nhttps://www.trendyol.com/sr?q=telefon\n/a-p-881465224\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	products, pages, err := ImportURLs(ctx, db, path)
	if err != nil {
		t.Fatal(err)
	}
	if products != 1 || pages != 1 {
		t.Fatalf("products=%d pages=%d", products, pages)
	}
}

func TestDiscoveryPriorityPrefersHighValueCategoriesWithoutDroppingOthers(t *testing.T) {
	phone, ok := ProductCandidate("https://www.trendyol.com/apple/iphone-17-cep-telefonu-p-881465224")
	if !ok || phone.Priority != 100 {
		t.Fatalf("phone candidate=%+v ok=%v", phone, ok)
	}
	underwear, ok := ProductCandidate("https://www.trendyol.com/marka/kadin-kulot-p-881465225")
	if !ok || underwear.Priority >= phone.Priority {
		t.Fatalf("underwear candidate=%+v ok=%v", underwear, ok)
	}
	if neutral := DiscoveryPriority("basic cotton product"); neutral != 0 {
		t.Fatalf("neutral priority=%d", neutral)
	}
}

func TestProgressiveSitemapRefillPersistsOffset(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	xml := `<?xml version="1.0"?><urlset><url><loc>https://www.trendyol.com/a-p-881465224</loc></url><url><loc>https://www.trendyol.com/b-p-881465225</loc></url><url><loc>https://www.trendyol.com/c-p-881465226</loc></url></urlset>`
	session := &fakeDiscoverySession{response: &client.Result{Status: 200, Body: []byte(xml)}}
	discoverer := &Discoverer{Store: db, Session: session, PendingLow: 2, PendingTarget: 2, ProductTarget: 100, SitemapMax: 301, Wake: make(chan struct{}, 1)}
	added, err := discoverer.Refill(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if added != 2 {
		t.Fatalf("added=%d", added)
	}
	offset, err := db.Metadata(ctx, "discovery_sitemap_offset")
	if err != nil || offset != "2" {
		t.Fatalf("offset=%q err=%v", offset, err)
	}
	if session.calls != 1 {
		t.Fatalf("sitemap calls=%d", session.calls)
	}
	pending, _ := db.Pending(ctx, "direct")
	if pending != 2 {
		t.Fatalf("pending=%d", pending)
	}
	if _, err := db.SQL().Exec(`UPDATE jobs SET state='done'`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	discoverer.PendingLow = 1
	discoverer.PendingTarget = 1
	added, err = discoverer.Refill(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Fatalf("second added=%d", added)
	}
	index, _ := db.Metadata(ctx, "discovery_sitemap_index")
	if !strings.EqualFold(index, "2") {
		t.Fatalf("index=%q", index)
	}
}

func TestFreshSitemapFloorRefillsDespiteHistoricalBacklog(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	historical := []store.Candidate{
		{ProductID: "1001", Path: "/old-a-p-1001", URL: "https://www.trendyol.com/old-a-p-1001"},
		{ProductID: "1002", Path: "/old-b-p-1002", URL: "https://www.trendyol.com/old-b-p-1002"},
		{ProductID: "1003", Path: "/old-c-p-1003", URL: "https://www.trendyol.com/old-c-p-1003"},
	}
	if _, err := db.Enqueue(ctx, historical); err != nil {
		t.Fatal(err)
	}
	xml := `<?xml version="1.0"?><urlset><url><loc>https://www.trendyol.com/fresh-p-2001</loc></url><url><loc>https://www.trendyol.com/fresh-p-2002</loc></url></urlset>`
	session := &fakeDiscoverySession{response: &client.Result{Status: 200, Body: []byte(xml)}}
	discoverer := &Discoverer{Store: db, Session: session, PendingLow: 2, PendingTarget: 2, FreshPendingLow: 1, FreshPendingTarget: 1, ProductTarget: 100, SitemapMax: 1}
	added, err := discoverer.Refill(ctx)
	if err != nil || added != 1 {
		t.Fatalf("added=%d err=%v", added, err)
	}
	var fresh int
	if err := db.SQL().QueryRow(`SELECT COUNT(*) FROM jobs WHERE priority>=200`).Scan(&fresh); err != nil || fresh != 1 {
		t.Fatalf("fresh=%d err=%v", fresh, err)
	}
}
