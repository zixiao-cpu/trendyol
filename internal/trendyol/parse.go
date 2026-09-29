package trendyol

import (
	"encoding/json"
	"fmt"
	stdhtml "html"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	nethtml "golang.org/x/net/html"

	"trendyol/internal/model"
)

type ParseInfo struct {
	Source              string
	VariationIncomplete bool
	MissingPriceSKUs    []string
}

type ParseError struct {
	Reason string
}

func (e *ParseError) Error() string { return e.Reason }

func ParseHTML(body []byte, productPath, scrapedAt string, prices PricePolicy, excluded []string, allowIncomplete bool) (*model.ProductBundle, ParseInfo, error) {
	state, _ := extractPageState(body)
	if state == nil {
		return nil, ParseInfo{}, &ParseError{Reason: "parser_incompatible"}
	}
	if state.Product == nil {
		if state.JSONLD != nil {
			bundle, err := parseJSONLD(state, productPath, scrapedAt, prices)
			return bundle, ParseInfo{Source: "json_ld"}, err
		}
		bundle, err := parseHTMLFallback(state, productPath, scrapedAt, prices)
		return bundle, ParseInfo{Source: "html_selector"}, err
	}
	return parseEmbedded(state, productPath, scrapedAt, prices, excluded, allowIncomplete)
}

func parseEmbedded(state *pageState, productPath, scrapedAt string, prices PricePolicy, excluded []string, allowIncomplete bool) (*model.ProductBundle, ParseInfo, error) {
	p := state.Product
	productID := clean(stringValue(p["id"]))
	if scrapedAt == "" {
		scrapedAt = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}
	path := canonicalPath(productPath, productID)
	brand := clean(stringValue(object(p["brand"])["name"]))
	images := normalizeImages(array(p["images"]))
	categoryNames, categoryIDs := categories(array(p["categoryTree"]))
	categoryPath := strings.Join(categoryNames, "|")
	for _, term := range excluded {
		if term != "" && strings.Contains(strings.ToLower(categoryPath), strings.ToLower(term)) {
			return nil, ParseInfo{}, &ParseError{Reason: "excluded_category"}
		}
	}
	features, specs, dimensions := attributes(array(p["attributes"]))
	if len(images) == 0 || productID == "" || clean(stringValue(p["name"])) == "" || brand == "" || len(categoryNames) == 0 {
		return nil, ParseInfo{}, &ParseError{Reason: "missing_required"}
	}
	product := model.Product{ProductID: productID, URL: "https://www.trendyol.com" + path, Title: clean(stringValue(p["name"])), Brand: brand, MainImage: images[0], Images: strings.Join(images, "|"), Description: productDetailText(state, p), Features: strings.Join(features, "|"), Specs: marshalObject(specs), CategoryPath: categoryPath, CategoryLevel1: at(categoryNames, 0), CategoryLevel2: at(categoryNames, 1), CategoryLevel3: at(categoryNames, 2), CategoryLeaf: at(categoryNames, len(categoryNames)-1), CategoryNodeIDs: strings.Join(categoryIDs, "|"), CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: scrapedAt}
	listing := object(p["merchantListing"])
	winner := object(listing["winnerVariant"])
	variants := mergeVariants(array(p["variants"]), array(listing["variants"]), winner, productID)
	info := ParseInfo{Source: "embedded_json"}
	groupID := clean(stringValue(p["productGroupId"]))
	if groupID == "" {
		groupID = productID
	}
	var output []model.Variant
	for _, variant := range variants {
		item := clean(stringValue(variant["itemNumber"]))
		if item == "" {
			continue
		}
		price, currency, ok := explicitPrice(variant)
		if !ok && len(variants) == 1 {
			price, currency, ok = explicitPrice(winner)
		}
		if !ok {
			info.MissingPriceSKUs = append(info.MissingPriceSKUs, item)
			continue
		}
		currency = defaultString(currency, "TRY")
		priceUSD, eligible := prices.USD(price, currency)
		if !eligible {
			if !strings.EqualFold(currency, "TRY") && !strings.EqualFold(currency, "TL") && !strings.EqualFold(currency, "USD") {
				return nil, info, &ParseError{Reason: "invalid_currency"}
			}
			continue
		}
		pairs, proven := variationPairs(p, variant, len(variants))
		status := "complete"
		rowGroup := groupID
		if len(variants) > 1 && !proven {
			info.VariationIncomplete = true
			status = "variation_incomplete"
			rowGroup = productID
			pairs = nil
			if !allowIncomplete {
				continue
			}
		}
		dims := copyMap(dimensions)
		applyDimensions(dims, pairs)
		themes, values := splitPairs(pairs)
		output = append(output, model.Variant{ItemNumber: item, ProductID: productID, ProductGroupID: rowGroup, Color: dims["color"], Capacity: dims["capacity"], Material: dims["material"], SellingPrice: fmt.Sprintf("%.2f", priceUSD), Currency: "USD", VariationTheme: strings.Join(themes, "|"), VariationValue: strings.Join(values, "|"), VariationStatus: status, CategoryKey: categoryKey(categoryIDs, categoryPath), ScrapedAt: scrapedAt})
	}
	info.MissingPriceSKUs = append([]string(nil), info.MissingPriceSKUs...)
	if info.VariationIncomplete && !allowIncomplete {
		return nil, info, &ParseError{Reason: "variation_incomplete"}
	}
	if len(output) == 0 {
		reason := "no_eligible_variants"
		if len(info.MissingPriceSKUs) > 0 {
			reason = "sku_price_missing"
		}
		return nil, info, &ParseError{Reason: reason}
	}
	if len(output) == 1 && output[0].VariationTheme == "" {
		output[0].ProductGroupID = productID
	}
	reviews := extractReviews(state.Root, 10)
	seller := extractSeller(listing)
	offers := extractOffers(listing, output, seller)
	bundle := &model.ProductBundle{Product: product, Variants: output, Seller: seller, Offers: offers, Reviews: reviews, VariationIncomplete: info.VariationIncomplete, MissingPriceSKUs: info.MissingPriceSKUs}
	return bundle, info, nil
}

func parseJSONLD(state *pageState, productPath, scrapedAt string, prices PricePolicy) (*model.ProductBundle, error) {
	data := state.JSONLD
	id := clean(first(data, "productID", "sku", "mpn"))
	if id == "" {
		return nil, &ParseError{Reason: "parser_incompatible"}
	}
	offer := object(data["offers"])
	price, ok := numberValue(offer["price"])
	if !ok {
		return nil, &ParseError{Reason: "no_eligible_variants"}
	}
	brand := clean(stringValue(object(data["brand"])["name"]))
	title := clean(stringValue(data["name"]))
	imgs := normalizeImages(anyArray(data["image"]))
	currency := defaultString(stringValue(offer["priceCurrency"]), "TRY")
	priceUSD, eligible := prices.USD(price, currency)
	if !eligible {
		if !strings.EqualFold(currency, "TRY") && !strings.EqualFold(currency, "TL") && !strings.EqualFold(currency, "USD") {
			return nil, &ParseError{Reason: "invalid_currency"}
		}
		return nil, &ParseError{Reason: "no_eligible_variants"}
	}
	if len(imgs) == 0 || title == "" || brand == "" {
		return nil, &ParseError{Reason: "missing_required"}
	}
	var cats []string
	state.Doc.Find(`.breadcrumb a, nav[aria-label*="breadcrumb" i] a`).Each(func(_ int, s *goquery.Selection) {
		if value := clean(s.Text()); value != "" {
			cats = append(cats, value)
		}
	})
	categoryPath := strings.Join(cats, "|")
	if categoryPath == "" {
		return nil, &ParseError{Reason: "parser_incompatible"}
	}
	if scrapedAt == "" {
		scrapedAt = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}
	path := canonicalPath(productPath, id)
	if value := stringValue(offer["url"]); value != "" {
		if u, err := url.Parse(value); err == nil && u.Path != "" {
			path = u.Path
		}
	}
	product := model.Product{ProductID: id, URL: "https://www.trendyol.com" + path, Title: title, Brand: brand, MainImage: imgs[0], Images: strings.Join(imgs, "|"), Description: productDetailText(state, nil), Features: "", Specs: "{}", CategoryPath: categoryPath, CategoryLevel1: at(cats, 0), CategoryLevel2: at(cats, 1), CategoryLevel3: at(cats, 2), CategoryLeaf: at(cats, len(cats)-1), CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: scrapedAt}
	variant := model.Variant{ItemNumber: id, ProductID: id, ProductGroupID: id, SellingPrice: fmt.Sprintf("%.2f", priceUSD), Currency: "USD", VariationStatus: "complete", CategoryKey: categoryPath, ScrapedAt: scrapedAt}
	return &model.ProductBundle{Product: product, Variants: []model.Variant{variant}}, nil
}

func parseHTMLFallback(state *pageState, productPath, scrapedAt string, prices PricePolicy) (*model.ProductBundle, error) {
	match := productURLPattern.FindStringSubmatch(productPath)
	if len(match) != 3 {
		return nil, &ParseError{Reason: "parser_incompatible"}
	}
	id := match[2]
	title := firstSelectionText(state, "h1", `[data-testid="product-name"]`)
	if title == "" {
		title = metaContent(state, "property", "og:title")
	}
	brand := firstSelectionText(state, `[data-testid="brand"]`, `[itemprop="brand"]`)
	if brand == "" {
		brand = metaContent(state, "property", "product:brand")
	}
	image := metaContent(state, "property", "og:image")
	priceText := metaContent(state, "property", "product:price:amount")
	if priceText == "" {
		priceText = firstSelectionAttrOrText(state, `[itemprop="price"]`, "content")
	}
	price, ok := numberValue(strings.ReplaceAll(priceText, ",", "."))
	if !ok {
		return nil, &ParseError{Reason: "no_eligible_variants"}
	}
	var categories []string
	state.Doc.Find(`.breadcrumb a, nav[aria-label*="breadcrumb"] a`).Each(func(_ int, s *goquery.Selection) {
		if value := clean(s.Text()); value != "" {
			categories = append(categories, value)
		}
	})
	if title == "" || brand == "" || image == "" || len(categories) == 0 {
		return nil, &ParseError{Reason: "parser_incompatible"}
	}
	currency := defaultString(metaContent(state, "property", "product:price:currency"), "TRY")
	priceUSD, eligible := prices.USD(price, currency)
	if !eligible {
		if !strings.EqualFold(currency, "TRY") && !strings.EqualFold(currency, "TL") && !strings.EqualFold(currency, "USD") {
			return nil, &ParseError{Reason: "invalid_currency"}
		}
		return nil, &ParseError{Reason: "no_eligible_variants"}
	}
	if scrapedAt == "" {
		scrapedAt = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}
	categoryPath := strings.Join(categories, "|")
	path := canonicalPath(productPath, id)
	product := model.Product{ProductID: id, URL: "https://www.trendyol.com" + path, Title: title, Brand: brand, MainImage: image, Images: image, Description: productDetailText(state, nil), Specs: "{}", CategoryPath: categoryPath, CategoryLevel1: at(categories, 0), CategoryLevel2: at(categories, 1), CategoryLevel3: at(categories, 2), CategoryLeaf: at(categories, len(categories)-1), CategorySource: "trendyol_category_tree", CategoryComplete: "true", ScrapedAt: scrapedAt}
	variant := model.Variant{ItemNumber: id, ProductID: id, ProductGroupID: id, SellingPrice: fmt.Sprintf("%.2f", priceUSD), Currency: "USD", VariationStatus: "complete", CategoryKey: categoryPath, ScrapedAt: scrapedAt}
	return &model.ProductBundle{Product: product, Variants: []model.Variant{variant}}, nil
}

func firstSelectionText(state *pageState, selectors ...string) string {
	for _, selector := range selectors {
		if value := clean(state.Doc.Find(selector).First().Text()); value != "" {
			return value
		}
	}
	return ""
}
func firstSelectionAttrOrText(state *pageState, selector, attribute string) string {
	selection := state.Doc.Find(selector).First()
	if value, ok := selection.Attr(attribute); ok && clean(value) != "" {
		return clean(value)
	}
	return clean(selection.Text())
}
func metaContent(state *pageState, attribute, value string) string {
	selection := state.Doc.Find(fmt.Sprintf(`meta[%s="%s"]`, attribute, value)).First()
	content, _ := selection.Attr("content")
	return clean(content)
}

// productDetailText preserves every readable product-detail value carried by
// the detail response while discarding scripts, page chrome and HTML markup.
// The fixed CSV contract keeps this complete plain-text representation in the
// description column; normalized attributes remain available in features and
// specs.
func productDetailText(state *pageState, product map[string]any) string {
	if state == nil {
		return ""
	}
	var sections []string
	seen := map[string]bool{}
	add := func(label, value string) {
		value = clean(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		sections = append(sections, "["+label+"] "+value)
	}

	if state.JSONLD != nil {
		add("summary", stringValue(state.JSONLD["description"]))
		for _, offer := range offerObjects(state.JSONLD["offers"]) {
			if shipping := offer["shippingDetails"]; shipping != nil {
				add("shipping", compactValue(shipping))
			}
			if returns := offer["hasMerchantReturnPolicy"]; returns != nil {
				add("returns", compactValue(returns))
			}
		}
	}

	if product != nil {
		var notes []string
		for _, raw := range array(product["attributes"]) {
			attribute := object(raw)
			note := clean(stringValue(attribute["description"]))
			if note == "" {
				continue
			}
			name := clean(stringValue(object(attribute["key"])["name"]))
			if name != "" {
				note = name + ": " + note
			}
			notes = append(notes, note)
		}
		add("attribute_notes", strings.Join(notes, " | "))
	}

	if state.Doc != nil {
		add("detail_page", detailPageSelectionText(state.Doc))
	}
	return strings.Join(sections, "\n")
}

func offerObjects(value any) []map[string]any {
	if offer := object(value); offer != nil {
		return []map[string]any{offer}
	}
	var out []map[string]any
	for _, raw := range array(value) {
		if offer := object(raw); offer != nil {
			out = append(out, offer)
		}
	}
	return out
}

func compactValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}

func detailPageSelectionText(doc *goquery.Document) string {
	if doc == nil {
		return ""
	}
	var root *goquery.Selection
	if wrapper := doc.Find(".productDetailWrapper").First(); wrapper.Length() > 0 {
		root = wrapper.Parent()
	} else {
		for _, selector := range []string{
			`[data-drroot="product-detail"]`,
			`[data-testid="product-info"]`,
			`.product-detail-container`,
		} {
			if selection := doc.Find(selector).First(); selection.Length() > 0 {
				root = selection
				break
			}
		}
	}
	if root == nil || root.Length() == 0 {
		return ""
	}
	clone := root.Clone()
	clone.Find("script,style,noscript,template,svg,iframe").Remove()
	var parts []string
	for _, node := range clone.Nodes {
		collectTextNodes(node, &parts)
	}
	return strings.Join(parts, " | ")
}

func collectTextNodes(node *nethtml.Node, parts *[]string) {
	if node == nil {
		return
	}
	if node.Type == nethtml.ElementNode {
		switch strings.ToLower(node.Data) {
		case "script", "style", "noscript", "template", "svg", "iframe":
			return
		}
	}
	if node.Type == nethtml.TextNode {
		value := clean(node.Data)
		if value != "" && (len(*parts) == 0 || (*parts)[len(*parts)-1] != value) {
			*parts = append(*parts, value)
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectTextNodes(child, parts)
	}
}

func mergeVariants(groups ...any) []map[string]any {
	merged := map[string]map[string]any{}
	var order []string
	productID := ""
	for _, group := range groups {
		switch value := group.(type) {
		case []any:
			for _, raw := range value {
				v := object(raw)
				item := stringValue(v["itemNumber"])
				if item == "" {
					continue
				}
				if merged[item] == nil {
					merged[item] = map[string]any{}
					order = append(order, item)
				}
				for k, x := range v {
					merged[item][k] = x
				}
			}
		case map[string]any:
			item := stringValue(value["itemNumber"])
			if item != "" {
				if merged[item] == nil {
					merged[item] = map[string]any{}
					order = append(order, item)
				}
				for k, x := range value {
					merged[item][k] = x
				}
			}
		case string:
			productID = value
		}
	}
	if len(order) == 0 && productID != "" {
		merged[productID] = map[string]any{"itemNumber": productID}
		order = append(order, productID)
	}
	out := make([]map[string]any, 0, len(order))
	for _, item := range order {
		out = append(out, merged[item])
	}
	return out
}

type pair struct{ Name, Value string }

func variationPairs(product, variant map[string]any, count int) ([]pair, bool) {
	slicing := object(variant["slicingAttributes"])
	// A product-level slicingAttributes value describes the currently selected
	// product.  It cannot prove the value of every sibling SKU on a multi-SKU
	// page, so only use it when there is a single SKU.
	if len(slicing) == 0 && count <= 1 {
		slicing = object(product["slicingAttributes"])
	}
	keys := make([]string, 0, len(slicing))
	for key := range slicing {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return dimensionPriority(keys[i]) < dimensionPriority(keys[j]) || dimensionPriority(keys[i]) == dimensionPriority(keys[j]) && keys[i] < keys[j]
	})
	pairs := variantAttributePairs(variant)
	for _, key := range keys {
		value := clean(stringValue(slicing[key]))
		if value != "" {
			pairs = upsertPair(pairs, pair{clean(key), value})
		}
	}
	value := clean(first(variant, "value", "beautifiedValue"))
	theme := clean(first(product, "variantAttributeName", "variantName"))
	if theme == "" && len(pairs) > 0 {
		theme = pairs[0].Name
	}
	if theme == "" {
		winner := object(object(product["merchantListing"])["winnerVariant"])
		if winnerPairs := variantAttributePairs(winner); len(winnerPairs) > 0 {
			theme = winnerPairs[0].Name
		}
	}
	if value != "" && theme != "" {
		pairs = upsertPair(pairs, pair{theme, value})
	}
	return pairs, count <= 1 || len(pairs) > 0
}

func variantAttributePairs(variant map[string]any) []pair {
	var pairs []pair
	for _, raw := range array(variant["variantAttributes"]) {
		attribute := object(raw)
		name := clean(first(attribute, "attributeName", "name"))
		value := clean(first(attribute, "attributeValue", "value"))
		if name != "" && value != "" {
			pairs = upsertPair(pairs, pair{Name: name, Value: value})
		}
	}
	return pairs
}

func upsertPair(pairs []pair, candidate pair) []pair {
	for i := range pairs {
		if normalizeName(pairs[i].Name) == normalizeName(candidate.Name) {
			pairs[i].Value = candidate.Value
			return pairs
		}
	}
	return append(pairs, candidate)
}

func explicitPrice(v map[string]any) (float64, string, bool) {
	p := object(v["price"])
	currency := stringValue(p["currency"])
	if selling := object(p["sellingPrice"]); selling != nil {
		value, ok := numberValue(selling["value"])
		return value, currency, ok
	}
	value, ok := numberValue(p["value"])
	return value, currency, ok
}

func attributes(raw []any) ([]string, map[string]string, map[string]string) {
	var features []string
	specs := map[string]string{}
	dims := map[string]string{"color": "", "capacity": "", "material": ""}
	for _, item := range raw {
		a := object(item)
		name := clean(stringValue(object(a["key"])["name"]))
		value := clean(stringValue(object(a["value"])["name"]))
		if name == "" || value == "" {
			continue
		}
		specs[name] = value
		features = append(features, name+": "+value)
		applyDimension(dims, name, value)
	}
	return features, specs, dims
}

func applyDimensions(dims map[string]string, pairs []pair) {
	for _, p := range pairs {
		applyDimension(dims, p.Name, p.Value)
	}
}
func applyDimension(dims map[string]string, name, value string) {
	n := normalizeName(name)
	switch {
	case containsAlias(n, []string{"dsmcolor", "color", "colour", "renk"}):
		dims["color"] = value
	case !strings.Contains(n, "ram") && containsAlias(n, []string{"internalmemory", "storagecapacity", "storagesize", "storage", "dahilihafiza", "hafizakapasitesi", "memorycapacity", "capacity", "kapasite", "hacim", "volume"}):
		dims["capacity"] = value
	case containsAlias(n, []string{"material", "materyal", "malzeme", "fabric", "kumas"}):
		dims["material"] = value
	}
}

func extractReviews(root map[string]any, limit int) []model.Review {
	var out []model.Review
	seen := map[string]bool{}
	var walk func(any)
	walk = func(value any) {
		if len(out) >= limit {
			return
		}
		switch value := value.(type) {
		case map[string]any:
			body := clean(first(value, "comment", "commentText", "reviewText", "review"))
			title := clean(stringValue(value["title"]))
			stars, ok := firstNumber(value, "rate", "rating", "stars", "score")
			if ok && stars >= 1 && stars <= 5 && (body != "" || title != "") {
				author := clean(first(value, "userFullName", "userName", "author", "customerName"))
				date := clean(first(value, "commentDateISOtype", "commentDate", "createdDate", "date"))
				key := body + "\x1f" + title + "\x1f" + date + "\x1f" + author
				if !seen[key] {
					seen[key] = true
					out = append(out, model.Review{Stars: stars, Title: title, Body: body, Date: date, Author: author, Images: reviewImages(value), Position: len(out)})
				}
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(root)
	return out
}

func extractSeller(listing map[string]any) *model.Seller {
	m := object(listing["merchant"])
	id := first(m, "id", "merchantId", "sellerId")
	if id == "" {
		return nil
	}
	return &model.Seller{SellerID: id, Name: clean(first(m, "name", "sellerName")), OfficialName: clean(stringValue(m["officialName"]))}
}
func extractOffers(listing map[string]any, variants []model.Variant, seller *model.Seller) []model.Offer {
	accepted := map[string]model.Variant{}
	for _, v := range variants {
		accepted[v.ItemNumber] = v
	}
	sources := append(array(listing["variants"]), listing["winnerVariant"])
	seen := map[string]bool{}
	var out []model.Offer
	for _, raw := range sources {
		v := object(raw)
		listingID := clean(stringValue(v["listingId"]))
		item := clean(stringValue(v["itemNumber"]))
		record, ok := accepted[item]
		if !ok || listingID == "" || seen[listingID] {
			continue
		}
		seen[listingID] = true
		sellerID := ""
		if seller != nil {
			sellerID = seller.SellerID
		}
		out = append(out, model.Offer{ListingID: listingID, ItemNumber: item, SellerID: sellerID, SellingPrice: record.SellingPrice, Currency: record.Currency})
	}
	return out
}

func categories(raw []any) ([]string, []string) {
	var names, ids []string
	complete := true
	for _, item := range raw {
		node := object(item)
		name := clean(stringValue(node["name"]))
		if name == "" {
			continue
		}
		names = append(names, name)
		id := clean(stringValue(node["id"]))
		if id == "" {
			complete = false
		}
		ids = append(ids, id)
	}
	if !complete {
		ids = nil
	}
	return names, ids
}
func normalizeImages(raw []any) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range raw {
		value := clean(stringValue(item))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "//") {
			value = "https:" + value
		}
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}
func anyArray(value any) []any {
	if a := array(value); a != nil {
		return a
	}
	if value != nil {
		return []any{value}
	}
	return nil
}
func reviewImages(value map[string]any) []string {
	for _, key := range []string{"mediaFiles", "images", "photos", "imageUrls"} {
		var raw []any
		if raw = array(value[key]); raw != nil {
			var out []string
			for _, item := range raw {
				if m := object(item); m != nil {
					item = first(m, "url", "path", "fullSizeUrl", "imageUrl")
				}
				out = append(out, normalizeImages([]any{item})...)
			}
			return out
		}
	}
	return []string{}
}
func splitPairs(pairs []pair) ([]string, []string) {
	themes := make([]string, 0, len(pairs))
	values := make([]string, 0, len(pairs))
	for _, p := range pairs {
		themes = append(themes, strings.ReplaceAll(p.Name, "|", " "))
		values = append(values, strings.ReplaceAll(p.Value, "|", " "))
	}
	return themes, values
}
func first(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringValue(m[key]); value != "" {
			return value
		}
	}
	return ""
}
func firstNumber(m map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		if value, ok := numberValue(m[key]); ok {
			return value, true
		}
	}
	return 0, false
}
func clean(value string) string {
	return strings.Join(strings.Fields(stdhtml.UnescapeString(value)), " ")
}
func canonicalPath(value, id string) string {
	if u, err := url.Parse(value); err == nil && u.Path != "" {
		value = u.Path
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	if !strings.Contains(value, "-p-"+id) {
		value = "/-p-" + id
	}
	return value
}
func at(values []string, index int) string {
	if index < 0 || index >= len(values) {
		return ""
	}
	return values[index]
}
func categoryKey(ids []string, path string) string {
	if len(ids) > 0 {
		return ids[len(ids)-1]
	}
	return path
}
func marshalObject(value map[string]string) string { raw, _ := json.Marshal(value); return string(raw) }
func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func copyMap(input map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range input {
		out[k] = v
	}
	return out
}
func normalizeName(value string) string {
	replacer := strings.NewReplacer("ı", "i", "ş", "s", "ğ", "g", "ü", "u", "ö", "o", "ç", "c", "İ", "i", "Ş", "s", "Ğ", "g", "Ü", "u", "Ö", "o", "Ç", "c")
	value = strings.ToLower(replacer.Replace(value))
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return -1
	}, value)
}
func containsAlias(value string, aliases []string) bool {
	for _, alias := range aliases {
		if value == alias || strings.Contains(value, alias) {
			return true
		}
	}
	return false
}
func dimensionPriority(name string) int {
	n := normalizeName(name)
	switch {
	case containsAlias(n, []string{"internalmemory", "capacity", "storage", "kapasite"}):
		return 0
	case containsAlias(n, []string{"color", "renk"}):
		return 1
	case containsAlias(n, []string{"material", "materyal"}):
		return 2
	}
	return 10
}
