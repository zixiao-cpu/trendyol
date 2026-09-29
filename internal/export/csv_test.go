package export

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"trendyol/internal/model"
	"trendyol/internal/store"
)

func TestStreamingExportQueriesReviewsOncePerProduct(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	candidate := store.Candidate{ProductID: "1", Path: "/product-p-1000", URL: "https://www.trendyol.com/product-p-1000"}
	_, _ = db.Enqueue(ctx, []store.Candidate{candidate})
	jobs, _ := db.BatchClaim(ctx, "direct", "d", 1, time.Minute)
	product := model.Product{ProductID: "1", URL: candidate.URL, Title: "Ürün", Brand: "Marka", MainImage: "https://cdn/main.jpg", Images: "https://cdn/main.jpg", Description: "[detail_page] Ürün açıklaması", Specs: "{}", CategoryPath: "Elektronik", CategoryLevel1: "Elektronik", CategoryLeaf: "Elektronik", CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: "2026-08-22T10:00:00Z"}
	variants := []model.Variant{{ItemNumber: "sku-1", ProductID: "1", ProductGroupID: "group", Color: "Siyah", SellingPrice: "60.00", Currency: "USD", VariationTheme: "Renk", VariationValue: "Siyah", VariationStatus: "complete", CategoryKey: "1", ScrapedAt: product.ScrapedAt}, {ItemNumber: "sku-2", ProductID: "1", ProductGroupID: "group", Color: "Mavi", SellingPrice: "70.00", Currency: "USD", VariationTheme: "Renk", VariationValue: "Mavi", VariationStatus: "complete", CategoryKey: "1", ScrapedAt: product.ScrapedAt}}
	bundle := &model.ProductBundle{Product: product, Variants: variants, Reviews: []model.Review{{Stars: 5, Body: "İyi", Images: []string{}}}}
	if err := db.ApplyResult(ctx, model.ScrapeResult{Job: jobs[0], Kind: model.ResultDone, Bundle: bundle}, 10, 10, 5, time.Second, filepath.Join(dir, "failures")); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "out", "TRENDYOL_SKU_DATA.csv")
	manifest, err := CSV(ctx, db, output, 20)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Rows != 2 || manifest.ReviewQueries != 1 || manifest.Bytes == 0 || len(manifest.SHA256) != 64 {
		t.Fatalf("manifest=%+v", manifest)
	}
	validated, err := ValidateFile(output, 20)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Rows != 2 || validated.SHA256 != manifest.SHA256 {
		t.Fatalf("validated=%+v manifest=%+v", validated, manifest)
	}
}
