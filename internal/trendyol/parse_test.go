package trendyol

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"trendyol/internal/model"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var testPricePolicy = PricePolicy{MinUSD: 20, TRYPerUSD: 48.066}

func TestPricePolicyConvertsTRYAndUsesStoredUSDRoundingForThreshold(t *testing.T) {
	if got, ok := testPricePolicy.USD(9899, "TRY"); !ok || got != 205.95 {
		t.Fatalf("TRY conversion got=%.2f eligible=%v", got, ok)
	}
	if got, ok := testPricePolicy.USD(20.004, "USD"); ok || got != 20 {
		t.Fatalf("rounded threshold got=%.2f eligible=%v", got, ok)
	}
	if got, ok := testPricePolicy.USD(20.006, "USD"); !ok || got != 20.01 {
		t.Fatalf("USD threshold got=%.2f eligible=%v", got, ok)
	}
}

func TestHistoricalSingleMatchesPythonBaseline(t *testing.T) {
	bundle, info, err := ParseHTML(
		fixture(t, "single_sku.html"),
		"/samsung/galaxy-a16-p-881465224",
		"2026-08-22T10:00:00Z",
		testPricePolicy,
		nil,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if info.Source != "embedded_json" || len(bundle.Variants) != 1 || len(bundle.Reviews) != 0 {
		t.Fatalf("unexpected parse info=%+v bundle=%+v", info, bundle)
	}
	file, err := os.Open(filepath.Join("..", "..", "testdata", "baseline", "historical_single.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows[0], model.CSVFields) {
		t.Fatalf("baseline header mismatch")
	}
	got := bundle.SKURecords("[]")[0]
	if !reflect.DeepEqual(got, rows[1]) {
		for index := range got {
			if got[index] != rows[1][index] {
				t.Errorf("field %s got=%q want=%q", model.CSVFields[index], got[index], rows[1][index])
			}
		}
	}
}

func TestDetailPageAllReadableContentIsStoredAsPlainText(t *testing.T) {
	body := string(fixture(t, "single_sku.html"))
	jsonLD := `<script type="application/ld+json">{"@type":"ProductGroup","description":"Kısa ürün tanıtımı","offers":{"shippingDetails":{"deliveryTime":{"handlingTime":{"minValue":"1","maxValue":"2"}}},"hasMerchantReturnPolicy":{"merchantReturnDays":15,"returnFees":"FreeReturn"}}}</script>`
	body = strings.Replace(body, "<body>", "<body>"+jsonLD, 1)
	body = strings.Replace(body, `"attributes":[{"key":{"name":"Renk"},"value":{"name":"Siyah"}}]`, `"attributes":[{"key":{"name":"Renk"},"value":{"name":"Siyah"},"description":"Bakım talimatı"}]`, 1)
	body = strings.Replace(body, "</body>", `<div class="content"><div class="productDetailWrapper"><h1>Ürün açıklaması</h1><div>Satıcı: Örnek Mağaza</div><div>İndirimli fiyat ve stok bilgisi</div></div><section><h2>Ek Bilgiler</h2><p>15 gün içinde ücretsiz iade</p><p>2 gün içinde kargoda</p></section></div></body>`, 1)
	bundle, _, err := ParseHTML([]byte(body), "/samsung/galaxy-a16-p-881465224", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"[summary] Kısa ürün tanıtımı",
		"[shipping]",
		"[returns]",
		"[attribute_notes] Renk: Bakım talimatı",
		"Ürün açıklaması",
		"Satıcı: Örnek Mağaza",
		"İndirimli fiyat ve stok bilgisi",
		"Ek Bilgiler",
		"15 gün içinde ücretsiz iade",
		"2 gün içinde kargoda",
	} {
		if !strings.Contains(bundle.Product.Description, expected) {
			t.Errorf("description missing %q: %s", expected, bundle.Product.Description)
		}
	}
	if strings.Contains(bundle.Product.Description, "<div") || strings.Contains(bundle.Product.Description, "<script") {
		t.Fatalf("description contains markup: %s", bundle.Product.Description)
	}
}

func TestMultiSKUUsesOwnPricesAndProvenVariations(t *testing.T) {
	bundle, info, err := ParseHTML(fixture(t, "multi_sku.html"), "/multi-p-900000001", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if info.VariationIncomplete || len(bundle.Variants) != 3 || len(bundle.Offers) != 3 || len(bundle.Reviews) != 2 {
		t.Fatalf("unexpected bundle: info=%+v variants=%d offers=%d reviews=%d", info, len(bundle.Variants), len(bundle.Offers), len(bundle.Reviews))
	}
	want := map[string]string{"700000001": "126.91", "700000002": "149.79", "700000003": "172.68"}
	for _, variant := range bundle.Variants {
		if variant.SellingPrice != want[variant.ItemNumber] {
			t.Errorf("sku %s got price %s", variant.ItemNumber, variant.SellingPrice)
		}
		if variant.VariationTheme == "" || variant.VariationValue == "" || variant.VariationTheme == "itemNumber" {
			t.Errorf("unproven/fake variation: %+v", variant)
		}
	}
	if bundle.Variants[2].Color != "Mavi" {
		t.Fatalf("variant-specific color lost: %+v", bundle.Variants[2])
	}
}

func TestMissingPriceNeverFallsBackToWinner(t *testing.T) {
	bundle, info, err := ParseHTML(fixture(t, "missing_price.html"), "/missing-p-900000002", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Variants) != 1 || bundle.Variants[0].ItemNumber != "710000001" {
		t.Fatalf("missing-price SKU should be omitted: %+v", bundle.Variants)
	}
	if !reflect.DeepEqual(info.MissingPriceSKUs, []string{"710000002"}) {
		t.Fatalf("missing price not reported: %+v", info)
	}
}

func TestMissingVariationRetriesThenDegradesWithoutFakeValue(t *testing.T) {
	_, info, err := ParseHTML(fixture(t, "missing_variant_value.html"), "/missing-p-900000003", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err == nil || !info.VariationIncomplete {
		t.Fatalf("first parse must request quality retry: info=%+v err=%v", info, err)
	}
	bundle, _, err := ParseHTML(fixture(t, "missing_variant_value.html"), "/missing-p-900000003", "2026-08-22T10:00:00Z", testPricePolicy, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Variants) != 2 {
		t.Fatalf("expected degraded SKU rows")
	}
	for _, variant := range bundle.Variants {
		if variant.VariationTheme != "" || variant.VariationValue != "" || variant.VariationStatus != "variation_incomplete" {
			t.Fatalf("fake variation generated: %+v", variant)
		}
	}
}

func TestMultiSKUDoesNotReuseProductLevelSlicingAttributes(t *testing.T) {
	product := map[string]any{
		"slicingAttributes":    map[string]any{"Renk": "Siyah"},
		"variantAttributeName": "Renk",
	}
	variant := map[string]any{"itemNumber": "sku-2"}
	if pairs, proven := variationPairs(product, variant, 2); proven || len(pairs) != 0 {
		t.Fatalf("product-level selected value must not prove sibling SKU: pairs=%+v proven=%v", pairs, proven)
	}
	variant["beautifiedValue"] = "Mavi"
	pairs, proven := variationPairs(product, variant, 2)
	if !proven || len(pairs) != 1 || pairs[0].Name != "Renk" || pairs[0].Value != "Mavi" {
		t.Fatalf("explicit variant value should prove SKU: pairs=%+v proven=%v", pairs, proven)
	}
}

func TestMultiSKUUsesWinnerVariantAttributeNameForSiblings(t *testing.T) {
	product := map[string]any{
		"merchantListing": map[string]any{
			"winnerVariant": map[string]any{
				"variantAttributes": []any{map[string]any{"attributeName": "Beden", "attributeValue": "S/M"}},
			},
		},
	}
	pairs, proven := variationPairs(product, map[string]any{"value": "L/XL"}, 4)
	if !proven || len(pairs) != 1 || pairs[0].Name != "Beden" || pairs[0].Value != "L/XL" {
		t.Fatalf("winner variant theme should prove sibling value: pairs=%+v proven=%v", pairs, proven)
	}
}

func TestJSONLDFallbackAndUnavailable(t *testing.T) {
	bundle, info, err := ParseHTML(fixture(t, "parser_changed.html"), "", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err != nil || info.Source != "json_ld" || bundle.Variants[0].SellingPrice != "156.04" || bundle.Variants[0].Currency != "USD" {
		t.Fatalf("JSON-LD fallback failed: info=%+v bundle=%+v err=%v", info, bundle, err)
	}
	if bundle.Product.CategorySource != "trendyol_category_tree" {
		t.Fatalf("unexpected category source %q", bundle.Product.CategorySource)
	}
	withoutBreadcrumb := map[string]any{
		"@context":  "https://schema.org",
		"@type":     "Product",
		"productID": "900000005",
		"name":      "Eksik Kategori",
		"brand":     map[string]any{"name": "Fixture Brand"},
		"image":     []string{"https://cdn.example/jsonld.jpg"},
		"offers":    map[string]any{"price": "7500.00", "priceCurrency": "TRY"},
	}
	raw, err := json.Marshal(withoutBreadcrumb)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(`<html><head><script type="application/ld+json">`), raw...)
	body = append(body, []byte(`</script></head><body><nav><a>unrelated</a></nav></body></html>`)...)
	if _, _, err := ParseHTML(body, "/missing-category-p-900000005", "2026-08-22T10:00:00Z", testPricePolicy, nil, false); err == nil || err.Error() != "parser_incompatible" {
		t.Fatalf("JSON-LD without a real breadcrumb must fail, got %v", err)
	}
	if _, _, err := ParseHTML(fixture(t, "unavailable.html"), "/unavailable-p-900000004", "2026-08-22T10:00:00Z", testPricePolicy, nil, false); err == nil {
		t.Fatal("unavailable fixture should not produce an eligible SKU")
	}
}

func TestHTMLSelectorFallback(t *testing.T) {
	bundle, info, err := ParseHTML(fixture(t, "html_fallback.html"), "/html-yedek-p-900000006", "2026-08-22T10:00:00Z", testPricePolicy, nil, false)
	if err != nil || info.Source != "html_selector" || bundle.Variants[0].SellingPrice != "158.12" || bundle.Product.CategoryLeaf != "Aksesuar" {
		t.Fatalf("HTML fallback failed: info=%+v bundle=%+v err=%v", info, bundle, err)
	}
	if bundle.Product.CategorySource != "trendyol_category_tree" {
		t.Fatalf("unexpected category source %q", bundle.Product.CategorySource)
	}
}

func TestChallengeClassificationIsExplicit(t *testing.T) {
	if got := ClassifyResponse(200, fixture(t, "challenge_200.html")); got != ResponseChallenge {
		t.Fatalf("challenge fixture classified as %s", got)
	}
	for _, status := range []int{403, 429, 503} {
		if got := ClassifyResponse(status, []byte("ordinary response")); got != ResponseRetry {
			t.Fatalf("ordinary HTTP %d must retry, got %s", status, got)
		}
	}
	if got := ClassifyResponse(404, fixture(t, "not_found_404.html")); got != ResponseNotFound {
		t.Fatalf("404 classified as %s", got)
	}
}

func TestClassifyCountrySelectionAsGeoGate(t *testing.T) {
	body := []byte(`<script>window.__STATE__={"pageType":"select_country"}</script>`)
	if got := ClassifyResponse(200, body); got != ResponseGeoGate {
		t.Fatalf("classification=%s", got)
	}
}

func TestExtractPuzzleSharedPropsAssignment(t *testing.T) {
	body := []byte(`<!doctype html><script>window["__envoy__SHARED_PROPS"]={"product":{"id":1234,"images":["https://cdn/image.jpg"],"categoryTree":[{"id":1,"name":"Elektronik"}],"merchantListing":{}}};</script>`)
	state, err := extractPageState(body)
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(state.Product["id"]); got != "1234" {
		t.Fatalf("product id=%s", got)
	}
}
