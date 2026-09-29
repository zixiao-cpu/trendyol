package trendyol

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"trendyol/internal/client"
	"trendyol/internal/store"
)

var productURLPattern = regexp.MustCompile(`(?i)(/[^\s"'<>]*?-p-(\d{4,}))`)

type discoverySession interface {
	Get(context.Context, string) (*client.Result, error)
	Rotate(string) error
}

type Discoverer struct {
	Store              *store.DB
	Session            discoverySession
	PendingLow         int
	PendingTarget      int
	FreshPendingLow    int
	FreshPendingTarget int
	ProductTarget      int
	SitemapMax         int
	Wake               chan<- struct{}
}

func ProductCandidate(value string) (store.Candidate, bool) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		if index := strings.Index(value, "trendyol.com"); index >= 0 {
			value = value[index+len("trendyol.com"):]
		} else {
			return store.Candidate{}, false
		}
	}
	match := productURLPattern.FindStringSubmatch(value)
	if len(match) != 3 {
		return store.Candidate{}, false
	}
	path := strings.Split(match[1], "?")[0]
	return store.Candidate{ProductID: match[2], Path: path, URL: "https://www.trendyol.com" + path, Priority: DiscoveryPriority(path)}, true
}

// ReprioritizeCandidates applies the current URL/category heuristic to an
// existing durable queue after an upgrade. Dataset imports can subsequently
// raise the score further with their title/category hints.
func ReprioritizeCandidates(ctx context.Context, db *store.DB) (int64, error) {
	rows, err := db.SQL().QueryContext(ctx, `SELECT product_id,path,priority FROM jobs`)
	if err != nil {
		return 0, err
	}
	type item struct {
		productID string
		priority  int
	}
	var changes []item
	for rows.Next() {
		var productID, path string
		var current int
		if err := rows.Scan(&productID, &path, &current); err != nil {
			rows.Close()
			return 0, err
		}
		priority := DiscoveryPriority(path)
		if current > 0 && current > priority {
			priority = current
		}
		if priority != current {
			changes = append(changes, item{productID: productID, priority: priority})
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, change := range changes {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET priority=? WHERE product_id=?`, change.priority, change.productID); err != nil {
			return 0, err
		}
	}
	return int64(len(changes)), tx.Commit()
}

func ImportURLs(ctx context.Context, db *store.DB, path string) (products, pages int64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	var candidates []store.Candidate
	var seedPages []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if candidate, ok := ProductCandidate(line); ok {
			candidate.Source = "import_urls"
			candidate.SourceRef = filepath.Base(path)
			candidates = append(candidates, candidate)
		} else if strings.Contains(line, "trendyol.com") || strings.HasPrefix(line, "/") {
			if strings.HasPrefix(line, "/") {
				line = "https://www.trendyol.com" + line
			}
			seedPages = append(seedPages, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	products, err = db.Enqueue(ctx, candidates)
	if err != nil {
		return 0, 0, err
	}
	pages, err = db.EnqueueSeedPages(ctx, seedPages)
	return products, pages, err
}

func ExtractProductLinks(body []byte) []store.Candidate {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []store.Candidate
	doc.Find("a[href],a[data-href]").Each(func(_ int, s *goquery.Selection) {
		value, ok := s.Attr("href")
		if !ok {
			value, _ = s.Attr("data-href")
		}
		if candidate, ok := ProductCandidate(value); ok && !seen[candidate.ProductID] {
			seen[candidate.ProductID] = true
			out = append(out, candidate)
		}
	})
	return out
}

func (d *Discoverer) Refill(ctx context.Context) (int64, error) {
	backfillActive, err := d.Store.ActiveContentBackfill(ctx)
	if err != nil {
		return 0, err
	}
	if backfillActive > 0 {
		return 0, nil
	}
	pending, err := d.Store.Pending(ctx, "direct")
	if err != nil {
		return 0, err
	}
	freshPending := int64(0)
	if d.FreshPendingLow > 0 {
		freshPending, err = d.Store.PendingAtLeastPriority(ctx, "direct", 200)
		if err != nil {
			return 0, err
		}
	}
	needsFresh := d.FreshPendingLow > 0 && int(freshPending) < d.FreshPendingLow
	if int(pending) >= d.PendingLow && !needsFresh {
		return 0, nil
	}
	products, err := d.Store.ProductCount(ctx)
	if err != nil {
		return 0, err
	}
	if d.ProductTarget > 0 && int(products) >= d.ProductTarget {
		return 0, nil
	}
	need := d.PendingTarget - int(pending)
	if needsFresh {
		need = d.FreshPendingTarget - int(freshPending)
	}
	if need < 1 {
		return 0, nil
	}
	index := metadataInt(ctx, d.Store, "discovery_sitemap_index", 1)
	offset := metadataInt(ctx, d.Store, "discovery_sitemap_offset", 0)
	if index > d.SitemapMax {
		if rawURL, ok, err := d.Store.ClaimSeedPage(ctx); err != nil {
			return 0, err
		} else if ok {
			return d.refillSeedPage(ctx, rawURL, need)
		}
		return 0, nil
	}
	response, err := d.Session.Get(ctx, fmt.Sprintf("https://www.trendyol.com/sitemap_products%d.xml", index))
	if err != nil {
		return 0, err
	}
	var discoveryAdded int64
	defer func() {
		_ = d.Store.RecordDiscovery(context.WithoutCancel(ctx), store.MetricDiscoverySitemap, discoveryAdded, response.Status, response.Bytes)
	}()
	if ClassifyResponse(response.Status, response.Body) != ResponseNormal {
		_ = d.Session.Rotate(fmt.Sprintf("sitemap_http_%d", response.Status))
		return 0, fmt.Errorf("sitemap %d http %d", index, response.Status)
	}
	var sitemap struct {
		Locations []string `xml:"url>loc"`
	}
	if err := xml.Unmarshal(response.Body, &sitemap); err != nil {
		return 0, err
	}
	var candidates []store.Candidate
	for position := offset; position < len(sitemap.Locations) && len(candidates) < need; position++ {
		if candidate, ok := ProductCandidate(sitemap.Locations[position]); ok {
			candidate.Source = "trendyol_sitemap"
			candidate.SourceRef = fmt.Sprintf("sitemap_products%d.xml", index)
			candidate.Priority = maxPriority(candidate.Priority, 300)
			candidates = append(candidates, candidate)
		}
		offset = position + 1
	}
	if offset >= len(sitemap.Locations) {
		index++
		offset = 0
	}
	if err := d.Store.SetMetadata(ctx, "discovery_sitemap_index", strconv.Itoa(index)); err != nil {
		return 0, err
	}
	if err := d.Store.SetMetadata(ctx, "discovery_sitemap_offset", strconv.Itoa(offset)); err != nil {
		return 0, err
	}
	added, err := d.Store.Enqueue(ctx, candidates)
	discoveryAdded = added
	if added > 0 {
		d.signal()
	}
	return added, err
}

func (d *Discoverer) refillSeedPage(ctx context.Context, rawURL string, need int) (int64, error) {
	response, err := d.Session.Get(ctx, rawURL)
	if err != nil {
		_ = d.Store.FinishSeedPage(ctx, rawURL, false, "network_error")
		return 0, err
	}
	var discoveryAdded int64
	defer func() {
		_ = d.Store.RecordDiscovery(context.WithoutCancel(ctx), store.MetricDiscoveryPublicPage, discoveryAdded, response.Status, response.Bytes)
	}()
	classification := ClassifyResponse(response.Status, response.Body)
	if classification != ResponseNormal {
		_ = d.Store.FinishSeedPage(ctx, rawURL, false, string(classification))
		return 0, fmt.Errorf("seed page %s: %s", rawURL, classification)
	}
	candidates := ExtractProductLinks(response.Body)
	for index := range candidates {
		candidates[index].Source = "trendyol_public_page"
		candidates[index].SourceRef = rawURL
		candidates[index].Priority = maxPriority(candidates[index].Priority, 200)
	}
	if len(candidates) > need {
		candidates = candidates[:need]
	}
	added, err := d.Store.Enqueue(ctx, candidates)
	discoveryAdded = added
	_ = d.Store.FinishSeedPage(ctx, rawURL, err == nil, errorText(err))
	if added > 0 {
		d.signal()
	}
	return added, err
}

func (d *Discoverer) Run(ctx context.Context) error {
	for {
		if _, err := d.Refill(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
func (d *Discoverer) signal() {
	if d.Wake == nil {
		return
	}
	select {
	case d.Wake <- struct{}{}:
	default:
	}
}
func metadataInt(ctx context.Context, db *store.DB, key string, fallback int) int {
	value, err := db.Metadata(ctx, key)
	if err == sql.ErrNoRows {
		return fallback
	}
	if err != nil {
		return fallback
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return number
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
